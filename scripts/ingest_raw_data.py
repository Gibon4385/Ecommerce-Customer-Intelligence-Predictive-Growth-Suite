import argparse
import os
import subprocess
import pandas as pd
from google.oauth2 import service_account
import google.auth
from google.cloud import storage
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ==================== Configuration ====================
# Load sensitive data from .env
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
PROJECT_ID = os.getenv("PROJECT_ID")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET")

if not all([PROJECT_ID, DATA_LAKE_BUCKET]):
    raise RuntimeError(
        "Error: Set PROJECT_ID and DATA_LAKE_BUCKET in .env"
    )

# Mapping of source files to BigQuery table names
DATA_FILES = {
    "data/events.csv": "events",
    "data/order_items.csv": "order_items",
    "data/orders.csv": "orders",
    "data/products.csv": "products",
    "data/reviews.csv": "reviews",
    "data/users.csv": "users",
}
# ===============================================


def main():
    parser = argparse.ArgumentParser(description="Upload historical CSV fixtures as raw Parquet")
    parser.add_argument(
        "--upload-with-gcloud",
        action="store_true",
        help="Use the active gcloud identity instead of SERVICE_ACCOUNT_JSON/ADC",
    )
    args = parser.parse_args()

    # Prefer Application Default Credentials in Google Cloud. A local key file
    # remains supported for development only.
    bucket = None
    if not args.upload_with_gcloud:
        if SERVICE_ACCOUNT_JSON:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_JSON
            )
        else:
            credentials, _ = google.auth.default()

        client = storage.Client(credentials=credentials, project=PROJECT_ID)
        bucket = client.bucket(DATA_LAKE_BUCKET)

    print(
        f"🚀 Uploading historical data to gs://{DATA_LAKE_BUCKET}/raw"
    )

    # Convert historical CSV files to Parquet in the same raw-zone layout used
    # by Structured Streaming, then upload them to GCS.
    errors = 0
    for file_name, table_name in DATA_FILES.items():
        if os.path.exists(file_name):
            print("--------------------------------------------------")
            print(f"📦 Detected file {file_name}, reading...")

            # Read CSV
            df = pd.read_csv(file_name, low_memory=False)

            # ELT best practice: Ingest raw data as-is
            destination = f"raw/{table_name}/historical.parquet"
            parquet_file = f"/tmp/{table_name}.parquet"
            print(f"📤 Uploading {len(df)} rows to gs://{DATA_LAKE_BUCKET}/{destination}")

            try:
                df.to_parquet(parquet_file, index=False)
                if args.upload_with_gcloud:
                    subprocess.run(
                        [
                            "gcloud", "storage", "cp", parquet_file,
                            f"gs://{DATA_LAKE_BUCKET}/{destination}",
                            f"--project={PROJECT_ID}",
                        ],
                        check=True,
                        text=True,
                        capture_output=True,
                    )
                else:
                    bucket.blob(destination).upload_from_filename(parquet_file)
                print(f"✅ {table_name} uploaded successfully!")
            except Exception as e:
                errors += 1
                print(f"❌ Error uploading {table_name}: {e}")
                if "403" in str(e):
                    print("Hint: grant Storage Object User on the data-lake bucket.")
        else:
            print(f"⚠️ File {file_name} not found, skipping.")

    if errors:
        print(f"\n❌ Historical ingestion failed for {errors} table(s).")
        return 1

    print("\n🎉 Historical data is now available in the GCS raw zone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
