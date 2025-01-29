# ETL Pipeline for Olist Store Dataset using AWS CDK

## Overview
This project provides an ETL (Extract, Transform, Load) pipeline built using AWS Cloud Development Kit (CDK) to analyze an e-commerce dataset from [Olist Store](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data). The dataset contains orders made at Olist Store, a Brazilian e-commerce platform, and includes customer, product, payment, review data, among others.

## Architecture
The ETL pipeline is deployed on AWS and utilizes the following services:

- **Amazon S3**: Stores raw and processed data.
- **AWS Glue**: Performs ETL transformations and crawling data.
- **Amazon Athena**: Used for querying transformed data.
- **AWS Glue Workflows**: Orchestrates the ETL workflow.
- **AWS IAM**: Used for establishing roles and permissions.

## Dataset Description
The Olist Store dataset includes multiple CSV files containing:
- Orders and customer data
- Product details and categories
- Seller information
- Payment transactions
- Order reviews

We will focus on using the next datasets:
- Orders
- Customers
- Products
- Order Items
- Payments

## Prerequisites
To deploy this project, you need:
- AWS CLI configured with appropriate permissions
- Node.js installed (for AWS CDK)
- AWS CDK installed (`npm install -g aws-cdk`)
- Python (if using AWS Glue Python scripts)

## Deployment Instructions
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd <project-folder>
   ```
2. **Install dependencies:**
   ```bash
   npm install
   ```
3. **Bootstrap CDK (if not already done):**
   ```bash
   cdk bootstrap
   ```
4. **Deploy the CDK Stack:**
   ```bash
   cdk deploy
   ```
5. **Verify the deployed resources in AWS Console.**

## Usage
Once deployed:
- Upload the dataset to the designated S3 bucket (this is done during the deployment automatically!)
- Trigger the ETL pipeline workflow manually or configure event-based triggers.
- Query transformed data using Amazon Athena.

## Cleanup
To remove deployed resources and avoid unnecessary costs:
```bash
cdk destroy
```

## Future Enhancements
- Implement automated data validation
- Integrate with AWS Glue DataBrew for data cleaning
- Add machine learning models for predictive analytics

## Additional resources
- [CDK Documentation](https://docs.aws.amazon.com/cdk/api/v2/)
- [DynamicFrame class](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html)
- [Using the Parquet format in AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html)
- [Glue Workflow AWS CDK](https://github.com/aws-samples/glue-workflow-aws-cdk)

## Contact
For questions or contributions, feel free to open an issue or reach out.

---
**Author:** Jano Camacho

