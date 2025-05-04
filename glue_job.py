import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read file from S3
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://bucket-for-glue-job-run/input/products_dataset.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

df = dyf.toDF()
df = df.groupBy("product_category_name").agg(count('*').alias("Count"))

# Coalesce and convert back to DynamicFrame
df = df.coalesce(1)
MyDynamicFrame = DynamicFrame.fromDF(df, glueContext, "test")

# Date-stamped output directory (not individual file name)
today_date = datetime.now().strftime("%Y-%m-%d")
output_path = f"s3://bucket-for-glue-job-run/output/output_file_{today_date}"

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=MyDynamicFrame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": output_path,
        "partitionKeys": [],
    },
    transformation_ctx="s3output",
)

job.commit()
