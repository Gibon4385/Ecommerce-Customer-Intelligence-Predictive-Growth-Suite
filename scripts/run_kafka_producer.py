import os
import sys
import json
import random
import time
import argparse
import logging
from datetime import datetime
from run_gap_filler import StatefulEcommerceGenerator

logger = logging.getLogger(__name__)


def start_streaming(generator, bootstrap_servers, delay):
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

    logger.info(
        f"🚀 Streaming simulator started in real-time. Daily user quota for today: {users_to_add_today}"
    )
    logger.info("Press Ctrl+C to stop.")

    total_messages = 0
    last_report_time = time.time()
    last_report_count = 0

    try:
        while True:
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

                producer.send("db_users", user)
                total_messages += 1
                logger.debug(
                    f"👤 USER SIGNUP: {user['user_id']} in {user['city']} (Today: {users_added_today}/{users_to_add_today})"
                )

            # User Event (View/Cart)
            elif roll < 0.70:
                if generator.active_users:
                    user_id = random.choice(generator.active_users)
                    event = generator.generate_event(user_id, now_str)

                    producer.send("db_events", event)
                    total_messages += 1
                    logger.debug(
                        f"👁️ EVENT: {user_id} {event['event_type'].upper()} {event['product_id']}"
                    )

            # User Checkout (Order + Order Items)
            elif roll < 0.90:
                if generator.active_users:
                    user_id = random.choice(generator.active_users)
                    order, items = generator.generate_order_and_items(user_id, now_str)

                    for item in items:
                        producer.send("db_order_items", item)
                        total_messages += 1
                        recent_purchased.append(item)
                        if len(recent_purchased) > 200:
                            recent_purchased.pop(0)

                    producer.send("db_orders", order)
                    total_messages += 1
                    logger.debug(
                        f"🛍️ ORDER PLACED: {order['order_id']} by {user_id} - {len(items)} items - Total ${order['total_amount']:.2f}"
                    )

            # User Review
            else:
                if recent_purchased:
                    purchase = random.choice(recent_purchased)
                    review = generator.generate_review(purchase, now_str)

                    producer.send("db_reviews", review)
                    total_messages += 1
                    logger.debug(
                        f'⭐ REVIEW: {purchase["user_id"]} rated {purchase["product_id"]} {review["rating"]} stars: "{review["review_text"]}"'
                    )
                    recent_purchased.remove(purchase)

            if random.random() < 0.20:
                producer.flush()

            # Periodic reporting at INFO level (prints every 5 seconds)
            elapsed = time.time() - last_report_time
            if elapsed >= 5.0:
                sent_since_last = total_messages - last_report_count
                current_eps = sent_since_last / elapsed
                logger.info(
                    f"📊 Sent: {total_messages:,} | Rate: {current_eps:.1f} EPS"
                )
                last_report_time = time.time()
                last_report_count = total_messages

            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("👋 Streaming simulator stopped by user.")
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Real-time Kafka Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: env KAFKA_BOOTSTRAP_SERVERS or localhost:9092)",
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

    start_streaming(generator, args.bootstrap_servers, args.delay)


if __name__ == "__main__":
    main()
