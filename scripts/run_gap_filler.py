import os
import csv
import sys
import random
import shutil
import argparse
from datetime import datetime, date, timedelta
from faker import Faker

# Base Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

USERS_CSV = os.path.join(DATA_DIR, "users.csv")
PRODUCTS_CSV = os.path.join(DATA_DIR, "products.csv")
ORDERS_CSV = os.path.join(DATA_DIR, "orders.csv")
ORDER_ITEMS_CSV = os.path.join(DATA_DIR, "order_items.csv")
EVENTS_CSV = os.path.join(DATA_DIR, "events.csv")
REVIEWS_CSV = os.path.join(DATA_DIR, "reviews.csv")

fake = Faker()


class StatefulEcommerceGenerator:
    def __init__(self):
        # 1. Load static products catalog
        self.products = self._load_products()

        # 2. Load metadata and unique cities
        self.cities = self._load_cities()

        # 3. Load active states
        self.active_users = self._load_active_users()
        self.purchased_items = self._load_purchased_items()

        # 4. Initialize ID counters
        self.max_user_idx = self._parse_max_id(USERS_CSV, "user_id", "U")
        self.max_order_idx = self._parse_max_id(ORDERS_CSV, "order_id", "O")
        self.max_order_item_idx = self._parse_max_id(
            ORDER_ITEMS_CSV, "order_item_id", "I"
        )
        self.max_event_idx = self._parse_max_id(EVENTS_CSV, "event_id", "E")
        self.max_review_idx = self._parse_max_id(REVIEWS_CSV, "review_id", "R")

        # Review templates
        self.review_templates = {
            1: [
                "Terrible product, broke immediately.",
                "Waste of money.",
                "Poor quality, would not buy again.",
            ],
            2: [
                "Color was different from images.",
                "Item arrived damaged.",
                "Not as expected, quality is poor.",
            ],
            3: [
                "Decent product for the price.",
                "Okay quality, average delivery speed.",
                "Product is fine, nothing special.",
            ],
            4: [
                "Highly recommend this brand.",
                "Value for money.",
                "Good product, fits description.",
            ],
            5: [
                "Excellent product, will buy again.",
                "Super fast shipping!",
                "Perfect, absolutely love it!",
            ],
        }

    def _parse_max_id(self, filepath, id_col, prefix):
        if not os.path.exists(filepath):
            return 0
        max_idx = 0
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    id_val = row.get(id_col)
                    if id_val and id_val.startswith(prefix):
                        try:
                            idx = int(id_val[len(prefix) :])
                            if idx > max_idx:
                                max_idx = idx
                        except ValueError:
                            pass
        except Exception as e:
            print(f"Warning reading {filepath}: {e}")
        return max_idx

    def _load_products(self):
        if not os.path.exists(PRODUCTS_CSV):
            # Cloud Run images intentionally do not contain the ignored local
            # history files. This small catalog lets the synthetic producer
            # bootstrap independently; historical data is loaded into GCS by
            # ingest_raw_data.py instead.
            return [
                {
                    "product_id": f"P{i:04d}",
                    "product_name": f"Demo Product {i}",
                    "category": ["Electronics", "Home", "Beauty", "Sports"][
                        (i - 1) % 4
                    ],
                    "brand": f"Demo Brand {((i - 1) % 5) + 1}",
                    "price": float(10 + i * 7),
                    "rating": round(3.5 + (i % 4) * 0.4, 1),
                }
                for i in range(1, 21)
            ]
        products = []
        with open(PRODUCTS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append(
                    {
                        "product_id": row["product_id"],
                        "product_name": row["product_name"],
                        "category": row["category"],
                        "brand": row["brand"],
                        "price": float(row["price"]),
                        "rating": float(row["rating"]),
                    }
                )
        return products

    def _load_cities(self):
        cities = set()
        if os.path.exists(USERS_CSV):
            try:
                with open(USERS_CSV, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        city = row.get("city")
                        if city:
                            cities.add(city)
            except Exception as e:
                print(f"Warning reading users.csv: {e}")
        if not cities:
            cities = {
                "Taipei",
                "New Taipei",
                "Taichung",
                "Tainan",
                "Kaohsiung",
                "Hsinchu",
            }
        return list(cities)

    def _load_active_users(self):
        users = []
        if os.path.exists(USERS_CSV):
            try:
                with open(USERS_CSV, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        user_id = row.get("user_id")
                        if user_id:
                            users.append(user_id)
            except Exception as e:
                print(f"Warning reading users.csv: {e}")
        return users

    def _load_purchased_items(self):
        purchases = []
        if os.path.exists(ORDER_ITEMS_CSV):
            try:
                with open(ORDER_ITEMS_CSV, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        purchases.append(
                            {
                                "user_id": row["user_id"],
                                "order_id": row["order_id"],
                                "product_id": row["product_id"],
                            }
                        )
            except Exception as e:
                print(f"Warning reading order_items.csv: {e}")
        return purchases

    def generate_user(self, date_str):
        self.max_user_idx += 1
        user_id = f"U{self.max_user_idx:06d}"
        name = fake.name()
        email = f"{name.lower().replace(' ', '.')}@example.{random.choice(['com', 'net', 'org'])}"
        gender = random.choices(
            ["Male", "Female", "Other"], weights=[0.45, 0.45, 0.10], k=1
        )[0]
        city = random.choice(self.cities)

        user_data = {
            "user_id": user_id,
            "name": name,
            "email": email,
            "gender": gender,
            "city": city,
            "signup_date": date_str,
        }
        self.active_users.append(user_id)
        return user_data

    def generate_event(self, user_id, timestamp_str):
        self.max_event_idx += 1
        event_id = f"E{self.max_event_idx:08d}"
        product = random.choice(self.products)
        event_type = random.choices(["view", "cart"], weights=[0.80, 0.20], k=1)[0]

        return {
            "event_id": event_id,
            "user_id": user_id,
            "product_id": product["product_id"],
            "event_type": event_type,
            "event_timestamp": timestamp_str,
        }

    def generate_order_and_items(self, user_id, timestamp_str):
        self.max_order_idx += 1
        order_id = f"O{self.max_order_idx:08d}"

        num_items = random.randint(1, 4)
        order_products = random.sample(self.products, num_items)

        item_records = []
        total_amount = 0.0

        for prod in order_products:
            self.max_order_item_idx += 1
            item_id = f"I{self.max_order_item_idx:08d}"
            quantity = random.randint(1, 3)
            price = prod["price"]
            item_total = round(price * quantity, 2)
            total_amount += item_total

            item_records.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "user_id": user_id,
                    "quantity": quantity,
                    "item_price": price,
                    "item_total": item_total,
                }
            )

            self.purchased_items.append(
                {
                    "user_id": user_id,
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "order_date": timestamp_str,
                }
            )

        status = random.choices(
            ["completed", "shipped", "processing", "cancelled", "returned"],
            weights=[0.60, 0.20, 0.10, 0.05, 0.05],
            k=1,
        )[0]

        order_record = {
            "order_id": order_id,
            "user_id": user_id,
            "order_date": timestamp_str,
            "order_status": status,
            "total_amount": round(total_amount, 2),
        }

        return order_record, item_records

    def generate_review(self, purchase, timestamp_str):
        self.max_review_idx += 1
        review_id = f"R{self.max_review_idx:08d}"
        rating = random.choices(
            [5, 4, 3, 2, 1], weights=[0.50, 0.30, 0.10, 0.05, 0.05], k=1
        )[0]
        text = random.choice(self.review_templates[rating])

        return {
            "review_id": review_id,
            "order_id": purchase["order_id"],
            "product_id": purchase["product_id"],
            "user_id": purchase["user_id"],
            "rating": rating,
            "review_text": text,
            "review_date": timestamp_str,
        }


def find_latest_date():
    latest = date(2024, 1, 1)

    # Read max date from orders
    if os.path.exists(ORDERS_CSV):
        try:
            with open(ORDERS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row.get("order_date")
                    if date_str:
                        try:
                            d_part = (
                                date_str.split("T")[0]
                                if "T" in date_str
                                else date_str.split(" ")[0]
                            )
                            d = datetime.strptime(d_part, "%Y-%m-%d").date()
                            if d > latest:
                                latest = d
                        except ValueError:
                            pass
        except Exception as e:
            print(f"Warning reading orders.csv: {e}")

    # Read max date from users
    if os.path.exists(USERS_CSV):
        try:
            with open(USERS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row.get("signup_date")
                    if date_str:
                        try:
                            d = datetime.strptime(date_str, "%Y-%m-%d").date()
                            if d > latest:
                                latest = d
                        except ValueError:
                            pass
        except Exception as e:
            print(f"Warning reading users.csv: {e}")
    return latest


def reset_data():
    backup_dir = os.path.join(DATA_DIR, "backup")
    if not os.path.exists(backup_dir):
        print(
            "❌ Error: Backup directory data/backup/ does not exist. Please run scripts/restore_and_backup.py first."
        )
        sys.exit(1)

    print("🔄 Resetting data files in data/ to original backup...")
    for filename in [
        "users.csv",
        "orders.csv",
        "order_items.csv",
        "events.csv",
        "reviews.csv",
        "products.csv",
    ]:
        src = os.path.join(backup_dir, filename)
        dst = os.path.join(DATA_DIR, filename)
        if os.path.exists(src):
            shutil.copyfile(src, dst)
            print(f"  ✅ Restored {filename}")
    print("🎉 All data files reset to original state!")


def fill_gap(generator):
    latest_date = find_latest_date()
    today = date.today()
    yesterday = today - timedelta(days=1)

    if latest_date >= yesterday:
        print(
            f"✅ No gap to fill. Data is already up to date (latest date: {latest_date})."
        )
        return

    print(f"⏳ Gap detected: from {latest_date + timedelta(days=1)} to {yesterday}.")

    # Open CSV files for appending
    f_users = open(USERS_CSV, "a", newline="", encoding="utf-8")
    f_events = open(EVENTS_CSV, "a", newline="", encoding="utf-8")
    f_orders = open(ORDERS_CSV, "a", newline="", encoding="utf-8")
    f_order_items = open(ORDER_ITEMS_CSV, "a", newline="", encoding="utf-8")
    f_reviews = open(REVIEWS_CSV, "a", newline="", encoding="utf-8")

    w_users = csv.writer(f_users)
    w_events = csv.writer(f_events)
    w_orders = csv.writer(f_orders)
    w_order_items = csv.writer(f_order_items)
    w_reviews = csv.writer(f_reviews)

    current_date = latest_date + timedelta(days=1)
    total_days = (yesterday - latest_date).days
    day_counter = 0

    # Keep track of purchased items for review generation during gap
    gap_purchased = []

    while current_date <= yesterday:
        date_str = current_date.strftime("%Y-%m-%d")
        day_counter += 1
        print(f"[{day_counter}/{total_days}] Generating data for {date_str}...")

        # Determine target active users count and event count based on user constraints:
        # "活躍用戶數 50 萬~150 萬用來反推: 總events ÷ 平均每人 40~80 次行為"
        # "Events 4,300 萬~1.73 億直接對應 500~2,000+ EPS"
        target_event_count = random.randint(43000000, 173000000)
        avg_actions = random.randint(40, 80)
        active_users_count = target_event_count // avg_actions

        # Keep active_users_count in 500,000 to 1,500,000 range
        if active_users_count < 500000:
            active_users_count = 500000
            avg_actions = target_event_count // active_users_count
        elif active_users_count > 1500000:
            active_users_count = 1500000
            avg_actions = target_event_count // active_users_count

        event_count = active_users_count * avg_actions

        # 1. Users: new signups (10,000 to 30,000 daily)
        new_users_count = random.randint(10000, 30000)
        for _ in range(new_users_count):
            user = generator.generate_user(date_str)
            w_users.writerow(
                [
                    user["user_id"],
                    user["name"],
                    user["email"],
                    user["gender"],
                    user["city"],
                    user["signup_date"],
                ]
            )

        # Cold start/population: ensure we have enough users in the pool to select active users
        if len(generator.active_users) < active_users_count:
            missing_users_count = active_users_count - len(generator.active_users)
            print(
                f"  Populating {missing_users_count} pre-existing users to meet active user pool size..."
            )
            for _ in range(missing_users_count):
                user = generator.generate_user(date_str)
                w_users.writerow(
                    [
                        user["user_id"],
                        user["name"],
                        user["email"],
                        user["gender"],
                        user["city"],
                        user["signup_date"],
                    ]
                )

        # 2. Events: 43M to 173M events
        print(
            f"  Generating {event_count} events for {active_users_count} active users..."
        )
        active_day_users = random.sample(generator.active_users, active_users_count)

        product_ids = [p["product_id"] for p in generator.products]
        event_types = ["view", "cart"]
        event_type_weights = [0.80, 0.20]

        chunk_size = 1000000
        num_chunks = (event_count + chunk_size - 1) // chunk_size

        for chunk_idx in range(num_chunks):
            current_chunk_size = min(chunk_size, event_count - chunk_idx * chunk_size)

            chunk_users = random.choices(active_day_users, k=current_chunk_size)
            chunk_products = random.choices(product_ids, k=current_chunk_size)
            chunk_types = random.choices(
                event_types, weights=event_type_weights, k=current_chunk_size
            )

            rows = []
            for i in range(current_chunk_size):
                generator.max_event_idx += 1
                event_id = f"E{generator.max_event_idx:08d}"

                rand_sec = random.randint(0, 86399)
                rand_micro = random.randint(0, 999999)
                timestamp_str = f"{date_str}T{rand_sec // 3600:02d}:{(rand_sec % 3600) // 60:02d}:{rand_sec % 60:02d}.{rand_micro:06d}"

                rows.append(
                    [
                        event_id,
                        chunk_users[i],
                        chunk_products[i],
                        chunk_types[i],
                        timestamp_str,
                    ]
                )
            w_events.writerows(rows)

        # 3. Orders & Order Items: 10,000 to 50,000 orders
        order_count = int(active_users_count * random.uniform(0.01, 0.03))
        order_count = max(10000, min(50000, order_count))
        print(f"  Generating {order_count} orders...")

        orders_rows = []
        items_rows = []
        for _ in range(order_count):
            user_id = random.choice(active_day_users)
            rand_sec = random.randint(0, 86399)
            rand_micro = random.randint(0, 999999)
            timestamp_str = f"{date_str}T{rand_sec // 3600:02d}:{(rand_sec % 3600) // 60:02d}:{rand_sec % 60:02d}.{rand_micro:06d}"

            order, items = generator.generate_order_and_items(user_id, timestamp_str)
            orders_rows.append(
                [
                    order["order_id"],
                    order["user_id"],
                    order["order_date"],
                    order["order_status"],
                    order["total_amount"],
                ]
            )
            for item in items:
                items_rows.append(
                    [
                        item["order_item_id"],
                        item["order_id"],
                        item["product_id"],
                        item["user_id"],
                        item["quantity"],
                        item["item_price"],
                        item["item_total"],
                    ]
                )

                order_datetime = datetime.combine(
                    current_date, datetime.min.time()
                ) + timedelta(seconds=rand_sec, microseconds=rand_micro)
                gap_purchased.append(
                    {
                        "user_id": item["user_id"],
                        "order_id": item["order_id"],
                        "product_id": item["product_id"],
                        "order_date": order_datetime,
                    }
                )
        w_orders.writerows(orders_rows)
        w_order_items.writerows(items_rows)

        # 4. Reviews: 10% to 20% of orders
        eligible_reviews = [
            p
            for p in gap_purchased
            if p["order_date"] < datetime.combine(current_date, datetime.max.time())
        ]
        if eligible_reviews:
            target_reviews_count = int(order_count * random.uniform(0.10, 0.20))
            num_reviews = min(len(eligible_reviews), target_reviews_count)
            print(f"  Generating {num_reviews} reviews...")
            to_review = random.sample(eligible_reviews, num_reviews)
            reviews_rows = []
            for purchase in to_review:
                offset = random.randint(1, 3)
                review_dt = purchase["order_date"] + timedelta(
                    days=offset, hours=random.randint(0, 5)
                )
                if review_dt.date() > yesterday:
                    review_dt = datetime.combine(yesterday, datetime.max.time())

                review = generator.generate_review(purchase, review_dt.isoformat())
                reviews_rows.append(
                    [
                        review["review_id"],
                        review["order_id"],
                        review["product_id"],
                        review["user_id"],
                        review["rating"],
                        review["review_text"],
                        review["review_date"],
                    ]
                )
            w_reviews.writerows(reviews_rows)

            # Efficiently remove selected reviews from gap_purchased
            to_review_set = set(id(p) for p in to_review)
            gap_purchased = [p for p in gap_purchased if id(p) not in to_review_set]

        current_date += timedelta(days=1)

    f_users.close()
    f_events.close()
    f_orders.close()
    f_order_items.close()
    f_reviews.close()
    print("🎉 Gap filling completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Gap Filler and Resetter")
    parser.add_argument(
        "--action",
        choices=["gap-fill", "reset"],
        default="gap-fill",
        help="Action to perform: 'gap-fill' (populate CSV missing history) or 'reset' (restore clean Kaggle data)",
    )
    args = parser.parse_args()

    if args.action == "reset":
        reset_data()
    else:
        print("📋 Initializing generator and metadata...")
        generator = StatefulEcommerceGenerator()
        print(
            f"📦 Loaded {len(generator.products)} products, {len(generator.cities)} cities, {len(generator.active_users)} active users."
        )
        fill_gap(generator)


if __name__ == "__main__":
    main()
