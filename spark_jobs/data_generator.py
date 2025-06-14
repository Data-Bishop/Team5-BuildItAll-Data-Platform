import argparse
import random
from datetime import datetime

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

# Initialize Faker
fake = Faker()
Faker.seed(42)

# Initialize SparkSession
spark = (
    SparkSession.builder.appName("SyntheticDataGeneration")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", 1)
    .config("spark.default.parallelism", 10)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .getOrCreate()
)


def generate_orders(region, n, customer_count=500_000):
    """Generate daily orders for a specific region as a Spark DataFrame."""
    try:
        data = [
            (
                i,
                random.randint(1, customer_count),
                fake.date_between(start_date="-1d", end_date="today"),
                round(random.uniform(20.0, 1000.0), 2),
                region
            )
            for i in range(1, n + 1)
        ]
        schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("customer_id", IntegerType(), False),
                StructField("order_date", DateType(), False),
                StructField("total", FloatType(), False),
                StructField("region", StringType(), False)
            ]
        )
        print(f"Generated {n} orders for region: {region}")
        return spark.createDataFrame(data, schema)
    except Exception as e:
        print(f"Error generating orders for region {region}: {e}")
        return None


def generate_order_items(region, n, order_count=1_000_000, product_count=800_000):
    """Generate daily order items for a specific region as a Spark DataFrame."""

    try:
        data = [
            (
                random.randint(1, order_count),
                random.randint(1, product_count),
                random.randint(1, 5),
                round(random.uniform(5.0, 300.0), 2),
                region
            )
            for _ in range(n)
        ]
        schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price", FloatType(), False),
                StructField("region", StringType(), False)
            ]
        )
        print(f"Generated {n} order items for region: {region}")
        return spark.createDataFrame(data, schema)
    except Exception as e:
        print(f"Error generating order items for region {region}: {e}")
        return None


def generate_payments(region, n, order_count=1_000_000):
    """Generate daily payments for a specific region as a Spark DataFrame."""
    try:
        methods = ["card", "bank", "transfer", "cash_on_delivery", "opay"]
        statuses = ["completed", "pending", "failed"]
        data = [
            (
                i,
                random.randint(1, order_count),
                random.choice(methods),
                random.choice(statuses),
                fake.date_time_between(start_date="-1d", end_date="now"),
                region
            )
            for i in range(1, n + 1)
        ]
        schema = StructType(
            [
                StructField("payment_id", IntegerType(), False),
                StructField("order_id", IntegerType(), False),
                StructField("method", StringType(), False),
                StructField("status", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("region", StringType(), False)
            ]
        )
        print(f"Generated {n} payments for region: {region}")
        return spark.createDataFrame(data, schema)
    except Exception as e:
        print(f"Error generating payments for region {region}: {e}")
        return None


def save_with_default_partitions(df, output_path, file_name):
    """Save a DataFrame as a single Parquet file directly to S3."""
    try:
        # Define the full S3 path
        s3_path = f"{output_path}{file_name}.parquet"
        df.write.mode("overwrite").parquet(s3_path)
        print(f"Saved {file_name}.parquet to {s3_path} (default partitions)")
    except Exception as e:
        print(f"Error saving DataFrame to S3: {e}")


if __name__ == "__main__":
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--output-path",
            required=True,
            help="S3 path to save the generated Parquet files"
        )
        args = parser.parse_args()

        # Output directory for the generated data
        output_path = args.output_path
        print(f"Output path: {output_path}")

        # Get today's date for file naming
        today = datetime.now().strftime("%Y-%m-%d")

        # Define regions or providers
        regions = ["Africa", "Europe", "Asia"]

        # Define record counts for each dataset
        record_counts = {
            "orders": [1_000_000, 800_000, 1_200_000],
            "order_items": [600_000, 500_000, 700_000],
            "payments": [500_000, 400_000, 600_000]
        }

        for i, region in enumerate(regions):
            # Generate datasets for each region with varying record counts
            orders = generate_orders(region, record_counts["orders"][i])
            order_items = generate_order_items(region, record_counts["order_items"][i])
            payments = generate_payments(region, record_counts["payments"][i])

            # Save datasets to Parquet files usingdefault partitions
            save_with_default_partitions(
                orders,
                output_path,
                f"orders_{region.replace(' ', '_')}_{today}"
            )
            save_with_default_partitions(
                order_items,
                output_path,
                f"order_items_{region.replace(' ', '_')}_{today}"
            )
            save_with_default_partitions(
                payments,
                output_path,
                f"payments_{region.replace(' ', '_')}_{today}"
            )

        print("Data generation and saving completed successfully.")
        # Stop SparkSession
        spark.stop()
    except Exception as e:
        print(f"Error in main execution: {e}")
        spark.stop()
