import os
import sys
import json
import random
import time
import argparse
import logging
import signal
import uuid
from pathlib import Path
from datetime import datetime
from run_gap_filler import StatefulEcommerceGenerator

logger = logging.getLogger(__name__)


def start_streaming(
    generator,
    bootstrap_servers,
    delay,
    target_eps=None,
    duration_seconds=None,
    report_interval=5.0,
    run_id=None,
    metrics_output=None,
):
    run_id = run_id or str(uuid.uuid4())
    stop_requested = False

    def request_stop(signum, _frame):
        nonlocal stop_requested
        logger.info("Received signal %s; finishing the current event...", signum)
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    # Dynamic JSON serializer selection for optimal performance
    try:
        import orjson

        def serializer(v):
            return orjson.dumps(v)

        logger.info("⚡ Using 'orjson' for ultra-fast serialization.")
    except ImportError:
        try:
            import ujson

            def serializer(v):
                return ujson.dumps(v).encode("utf-8")

            logger.info("⚡ Using 'ujson' for fast serialization.")
        except ImportError:

            def serializer(v):
                return json.dumps(v).encode("utf-8")

            logger.info(
                "ℹ️ Using standard 'json' for serialization. (Install 'orjson' for faster speed)"
            )

    logger.info(f"🔌 Connecting to Kafka broker at {bootstrap_servers}...")
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=serializer,
            max_block_ms=5000,
            api_version=(3, 0, 0),
            # Performance tuning configurations
            linger_ms=10,  # Batch messages sent within 10ms
            batch_size=65536,  # 64KB batch size
            acks=1,  # Wait for leader acknowledgment
            compression_type=None,  # Disabled on localhost to save CPU cycles
        )
        logger.info("✅ Connected to Kafka successfully!")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        logger.error(
            "Please check if your Docker containers are running (docker compose ps)."
        )
        sys.exit(1)

    # We keep a rolling cache of recent purchases to review in real-time
    recent_purchased = (
        list(generator.purchased_items[-100:]) if generator.purchased_items else []
    )

    last_user_added_day = datetime.now().date()
    users_to_add_today = random.randint(0, 200)
    users_added_today = 0
    total_messages = 0
    topic_counts = {
        "db_users": 0,
        "db_events": 0,
        "db_orders": 0,
        "db_order_items": 0,
        "db_reviews": 0,
    }
    delivery_errors = 0

    def record_delivery_error(error):
        nonlocal delivery_errors
        delivery_errors += 1
        logger.error("Kafka delivery failed: %s", error)

    def send(topic, value):
        nonlocal total_messages
        future = producer.send(topic, value)
        future.add_errback(record_delivery_error)
        total_messages += 1
        topic_counts[topic] += 1

    # A cloud image starts without the ignored local CSV history. Seed one user
    # so event and order generation can begin immediately.
    if not generator.active_users:
        initial_user = generator.generate_user(last_user_added_day.isoformat())
        send("db_users", initial_user)
        users_added_today = 1
        users_to_add_today = max(users_to_add_today, 1)
        logger.info("Seeded initial synthetic user %s", initial_user["user_id"])

    logger.info(
        f"🚀 Streaming simulator started in real-time. Daily user quota for today: {users_to_add_today}"
    )
    logger.info("Press Ctrl+C to stop.")
    logger.info("Load-test run ID: %s", run_id)

    started_at = datetime.now().astimezone()
    start_time = time.monotonic()
    last_report_time = start_time
    last_report_count = 0

    try:
        while not stop_requested:
            loop_started = time.monotonic()
            if duration_seconds is not None and loop_started - start_time >= duration_seconds:
                logger.info("Configured duration reached; stopping run %s.", run_id)
                break

            now = datetime.now()
            now_str = now.isoformat()
            today = now.date()

            if today != last_user_added_day:
                last_user_added_day = today
                users_to_add_today = random.randint(0, 200)
                users_added_today = 0
                logger.info(
                    f"📅 New day detected! User quota for {today}: {users_to_add_today}"
                )

            roll = random.random()

            # User Registration (if quota remains)
            if roll < 0.10 and users_added_today < users_to_add_today:
                user = generator.generate_user(today.strftime("%Y-%m-%d"))
                users_added_today += 1

                send("db_users", user)
                logger.debug(
                    f"👤 USER SIGNUP: {user['user_id']} in {user['city']} (Today: {users_added_today}/{users_to_add_today})"
                )

            # User Event (View/Cart)
            elif roll < 0.70:
                if generator.active_users:
                    user_id = random.choice(generator.active_users)
                    event = generator.generate_event(user_id, now_str)

                    send("db_events", event)
                    logger.debug(
                        f"👁️ EVENT: {user_id} {event['event_type'].upper()} {event['product_id']}"
                    )

            # User Checkout (Order + Order Items)
            elif roll < 0.90:
                if generator.active_users:
                    user_id = random.choice(generator.active_users)
                    order, items = generator.generate_order_and_items(user_id, now_str)

                    for item in items:
                        send("db_order_items", item)
                        recent_purchased.append(item)
                        if len(recent_purchased) > 200:
                            recent_purchased.pop(0)

                    send("db_orders", order)
                    logger.debug(
                        f"🛍️ ORDER PLACED: {order['order_id']} by {user_id} - {len(items)} items - Total ${order['total_amount']:.2f}"
                    )

            # User Review
            else:
                if recent_purchased:
                    purchase = random.choice(recent_purchased)
                    review = generator.generate_review(purchase, now_str)

                    send("db_reviews", review)
                    logger.debug(
                        f'⭐ REVIEW: {purchase["user_id"]} rated {purchase["product_id"]} {review["rating"]} stars: "{review["review_text"]}"'
                    )
                    recent_purchased.remove(purchase)

            if random.random() < 0.20:
                producer.flush()

            # Periodic reporting at INFO level (prints every 5 seconds)
            current_time = time.monotonic()
            elapsed = current_time - last_report_time
            if elapsed >= report_interval:
                sent_since_last = total_messages - last_report_count
                current_eps = sent_since_last / elapsed
                logger.info(
                    f"📊 Sent: {total_messages:,} | Rate: {current_eps:.1f} EPS"
                )
                logger.info(
                    "PIPELINE_HEARTBEAT component=producer "
                    f"run_id={run_id or 'continuous'} "
                    f"messages_total={total_messages} rate_eps={current_eps:.1f}"
                )
                last_report_time = current_time
                last_report_count = total_messages

            if target_eps:
                # Pace against actual Kafka messages, not simulator iterations;
                # an order can emit one order plus several order-item messages.
                target_elapsed = total_messages / target_eps
                sleep_seconds = target_elapsed - (time.monotonic() - start_time)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
            elif delay > 0:
                time.sleep(delay)

    finally:
        logger.info("👋 Streaming simulator stopping; flushing buffered messages.")
        producer.flush()
        producer.close()
        ended_at = datetime.now().astimezone()
        elapsed_seconds = max(time.monotonic() - start_time, 0.000001)
        summary = {
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_seconds": round(elapsed_seconds, 3),
            "target_eps": target_eps,
            "actual_eps": round(total_messages / elapsed_seconds, 3),
            "messages_sent": total_messages,
            "delivery_errors": delivery_errors,
            "topic_counts": topic_counts,
            "bootstrap_servers": bootstrap_servers,
        }
        logger.info("LOAD_TEST_SUMMARY %s", json.dumps(summary, sort_keys=True))
        if metrics_output:
            output_path = Path(metrics_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            logger.info("Wrote load-test summary to %s", output_path)


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Real-time Kafka Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: env KAFKA_BOOTSTRAP_SERVERS or localhost:9092)",
    )
    parser.add_argument(
        "--target-eps",
        type=float,
        help="Target actual Kafka messages per second; overrides --delay when set",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        help="Stop after this many seconds; omit for continuous operation",
    )
    parser.add_argument(
        "--report-interval",
        type=float,
        default=5.0,
        help="Seconds between heartbeat reports (default: 5)",
    )
    parser.add_argument("--run-id", help="Stable identifier included in the test summary")
    parser.add_argument(
        "--metrics-output",
        help="Optional path for the final JSON run summary",
    )
    parser.add_argument(
        "--seed", type=int, help="Optional random seed for repeatable test data selection"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay between streamed events in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO). Use DEBUG to see every generated event on stdout.",
    )
    args = parser.parse_args()
    if args.target_eps is not None and args.target_eps <= 0:
        parser.error("--target-eps must be greater than zero")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    if args.report_interval <= 0:
        parser.error("--report-interval must be greater than zero")
    if args.seed is not None:
        random.seed(args.seed)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("📋 Initializing generator and loading baseline data...")
    generator = StatefulEcommerceGenerator()
    logger.info(
        f"📦 Loaded {len(generator.products)} products, {len(generator.cities)} cities, {len(generator.active_users)} active users."
    )

    start_streaming(
        generator,
        args.bootstrap_servers,
        args.delay,
        target_eps=args.target_eps,
        duration_seconds=args.duration_seconds,
        report_interval=args.report_interval,
        run_id=args.run_id,
        metrics_output=args.metrics_output,
    )


if __name__ == "__main__":
    main()
