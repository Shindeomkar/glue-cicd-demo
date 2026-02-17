from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DemoJob").getOrCreate()

# Read CSV from S3
df = spark.read.csv("s3://input-data-omkar/data/", header=True)

# Remove empty rows
clean_df = df.dropna()

# Save output
clean_df.write.mode("overwrite").parquet(
    "s3://output-data-omkar/output/"
)

print("Pipeline Finished")
