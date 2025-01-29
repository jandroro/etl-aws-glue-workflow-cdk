import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## ============================================
## SETUP THE GLUE CONTEXT
## ============================================

# Retrieve parameters from the Glue Job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SALES_PARAMS'])

# Extract parameter values
sales_params = json.loads(args['SALES_PARAMS'])
source_db = sales_params['source_db']
target_bucket = sales_params['target_bucket']

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize Glue Job
job.init(args['JOB_NAME'], args)

# Set the target file size (e.g., 128 MB in bytes)
target_file_size = 128 * 1024 * 1024  # 128 MB

## ============================================
## METHODS TO BE USED
## ============================================

# Method for getting the number of partititions
def get_num_partitions(target_file_size, df):
    # Calculate the number of partitions based on the target file size
    num_partitions = int(
        df.count() / (target_file_size / df.rdd.map(lambda row: len(str(row))).mean())
    )
    
    if num_partitions == 0:
        num_partitions = 1  # Ensure at least one partition
    
    return num_partitions

## ============================================
## READ DATA AS DYNAMIC FRAMES
## ============================================

# DynamicFrame - Order Items
dyf_order_items = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="order_items",
    transformation_ctx="dyf_order_items"
).select_fields(["order_id", "order_item_id", "product_id", "shipping_limit_date", "price", "freight_value"])

# DynamicFrame - Orders
dyf_orders = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="orders",
     transformation_ctx="dyf_orders"
).select_fields(["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"])

# DynamicFrame - Products
dyf_products = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="products",
    transformation_ctx="dyf_products"
).select_fields(["product_id", "product_category_name"])

# DynamicFrame - Customers
dyf_customers = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="customers",
    transformation_ctx="dyf_customers"
).select_fields(["customer_id", "customer_unique_id", "customer_city"])

# DynamicFrame - Order Payments
dyf_order_payments = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="order_payments",
    transformation_ctx="dyf_order_payments"
).select_fields(["order_id", "payment_type", "payment_value"])

## ============================================
## MAKE JOINS
## ============================================

# Notes:
# DynamicFrames don't support LEFT and RIGHT JOINS in a native way.
# For it, you need to convert them to DataFrames and perform the join operation.
# After the join operation, you convert the DataFrame back to a DynamicFrame.

# Build a Orders Joined Dataframe
df_sales_joined = dyf_order_items.toDF() \
    .join(dyf_orders.toDF(), on="order_id", how="left") \
    .join(dyf_products.toDF(), on="product_id", how="left") \
    .join(dyf_customers.toDF(), on="customer_id", how="left") \
    .join(dyf_order_payments.toDF(), on="order_id", how="left")

# Repartitioning the Orders Joined DataFrame based on the target file size
df_sales_joined = df_sales_joined.repartition(get_num_partitions(target_file_size, df_sales_joined))

# Convert back to DynamicFrame and select the desired columns
dyf_sales_joined = DynamicFrame.fromDF(df_sales_joined, glueContext) \
    .select_fields([
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "payment_type",
        "payment_value",
        "customer_unique_id",
        "customer_city",
        "order_item_id",
        "product_category_name",
        "shipping_limit_date",
        "price",
        "freight_value"
    ])

## ============================================
## WRITE DATA TO PARQUET FORMAT
## ============================================

glueContext.write_dynamic_frame.from_options(
    frame=dyf_sales_joined, 
    connection_type="s3", 
    connection_options={
        "path": f"s3://{target_bucket}/sales/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="SalesParquet"
)

job.commit()