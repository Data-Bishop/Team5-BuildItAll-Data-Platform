import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize SparkSession
spark = (
    SparkSession.builder.appName("DataProcessing")
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


def process_and_union_files(input_path, output_path, dataset_name):
    """
    Process and union all Parquet files for a specific dataset and save as a single Parquet file.
    Args:
        input_path (str): Base S3 input path.
        output_path (str): Base S3 output path.
        dataset_name (str): Dataset name (e.g., "orders", "order_items", "payments").
        date (str): Date in YYYY-MM-DD format.
    """
    try:
        # Define the S3 path for the dataset
        dataset_path = f"{input_path}{dataset_name}_*.parquet"
        print(f"Reading files from: {dataset_path}")

        # Read all Parquet files for the dataset
        df = spark.read.parquet(dataset_path)

        # Add a column to track the dataset name (optional, for debugging)
        df = df.withColumn("source_dataset", lit(dataset_name))

        # Write the unioned DataFrame back to S3 as a single Parquet file
        output_file_path = f"{output_path}{dataset_name}.parquet"
        df.write.mode("overwrite").parquet(output_file_path)

        print(f"Processed and saved {dataset_name} to: {output_file_path}")
    except Exception as e:
        print(f"Error processing {dataset_name}: {e}")


if __name__ == "__main__":
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--input-path",
            required=True,
            help="Base S3 input path for the Parquet files"
        )
        parser.add_argument(
            "--output-path",
            required=True,
            help="Base S3 output path for the processed files"
        )
        args = parser.parse_args()

        # Input and output paths
        input_path = args.input_path
        output_path = args.output_path
        print(f"Input path: {input_path}")
        print(f"Output path: {output_path}")

        # Process each dataset
        datasets = ["orders", "order_items", "payments"]
        for dataset in datasets:
            process_and_union_files(input_path, output_path, dataset)

        print("Data processing completed successfully.")
        # Stop SparkSession
        spark.stop()
    except Exception as e:
        print(f"Error in main execution: {e}")
        spark.stop()
