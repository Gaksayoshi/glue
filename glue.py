import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define S3 input/output paths
input_s3_path = "s3://your-input-bucket/data/"
output_s3_path = "s3://your-output-bucket/transformed-data/"

# Read data from S3 as a Glue DynamicFrame
dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": "true"},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [input_s3_path]},
)

# Convert to Spark DataFrame for transformations
df = dyf.toDF()

# Example transformation: Drop a column
df_transformed = df.drop("unwanted_column")

# Convert back to DynamicFrame
dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext)

# Write transformed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=dyf_transformed,
    connection_type="s3",
    format="parquet",
    connection_options={"path": output_s3_path},
)

print("Glue Job Completed Successfully!")
