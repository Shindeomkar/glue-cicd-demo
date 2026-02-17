from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DemoJob").getOrCreate()

# Read CSV from S3
df = spark.read.csv("s3://YOUR-INPUT-BUCKET/data/", header=True)

# Remove empty rows
clean_df = df.dropna()

# Save output
clean_df.write.mode("overwrite").parquet(
    "s3://YOUR-OUTPUT-BUCKET/output/"
)

print("Done")
