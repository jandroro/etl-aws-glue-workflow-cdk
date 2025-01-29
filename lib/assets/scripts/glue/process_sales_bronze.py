import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

## ============================================
## READ DATA AS DYNAMIC FRAMES
## ============================================

# DynamicFrame - Order Items
dyf_order_items = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="order_items",
    transformation_ctx="dyf_order_items"
)

# DynamicFrame - Orders
dyf_orders = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="orders",
    transformation_ctx="dyf_orders"
)

# DynamicFrame - Products
dyf_products = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="products",
    transformation_ctx="dyf_products"
)

# DynamicFrame - Customers
dyf_customers = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="customers",
    transformation_ctx="dyf_customers"
)

# DynamicFrame - Order Payments
dyf_order_payments = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="order_payments",
    transformation_ctx="dyf_order_payments"
)

## ============================================
## WRITE DATA TO PARQUET FORMAT
## ============================================

# Order Items Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_order_items,
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/order_items/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="OrderItemsParquet"
)

# Orders Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_orders,
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/orders/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="OrdersParquet"
)

# Products Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_products,
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/products/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="ProductsParquet"
)

# Customers Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_customers,
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/customers/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="CustomersParquet"
)

# Order Payments Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_order_payments,
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/order_payments/"
    },
    format="glueparquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="OrderPaymentsParquet"
)

job.commit()