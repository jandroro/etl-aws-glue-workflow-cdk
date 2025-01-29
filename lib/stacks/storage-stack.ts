import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3Deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as glue from 'aws-cdk-lib/aws-glue';

export class StorageStack extends cdk.NestedStack {
    public readonly bronzeBucketName: string;
    public readonly silverBucketName: string;
    public readonly goldBucketName: string;
    public readonly resourcesBucketName: string;
    public readonly salesBronzeDbName: string;
    public readonly salesSilverDbName: string;
    public readonly salesGoldDbName: string;

    constructor(scope: Construct, id: string, props?: cdk.NestedStackProps) {
        super(scope, id, props);

        // ********************************************
        // S3 BUCKETS
        // ********************************************

        // S3 Bucket - Bronze
        const bronzeBucket = new s3.Bucket(this, 'S3BucketSalesBronze', {
            bucketName: 'lab-glue-workshop-sales-bronze',
            autoDeleteObjects: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        });

        // Upload local assests/data to S3 Bucket Bronze
        new s3Deploy.BucketDeployment(this, 'S3BucketSalesDataBronzeDeployment', {
            sources: [
                s3Deploy.Source.asset('./lib/assets/data')
            ],
            destinationBucket: bronzeBucket
        });

        // S3 Bucket - Silver
        const silverBucket = new s3.Bucket(this, 'S3BucketSalesSilver', {
            bucketName: 'lab-glue-workshop-sales-silver',
            autoDeleteObjects: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        });

        // S3 Bucket - Gold
        const goldBucket = new s3.Bucket(this, 'S3BucketSalesGold', {
            bucketName: 'lab-glue-workshop-sales-gold',
            autoDeleteObjects: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        });

        // S3 Bucket - Resources
        const resourcesBucket = new s3.Bucket(this, 'S3BucketSalesResources', {
            bucketName: 'lab-glue-workshop-sales-resources',
            autoDeleteObjects: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        });

        // Upload Glue Job Codes to S3 Bucket Resources
        new s3Deploy.BucketDeployment(this, 'S3BucketSalesJobProcessBronzeDeployment', {
            sources: [
                s3Deploy.Source.asset('./lib/assets/scripts')
            ],
            destinationBucket: resourcesBucket,
            destinationKeyPrefix: 'scripts'
        });

        // ********************************************
        // GLUE DATABASES
        // ********************************************

        // Glue Database - Bronze
        const salesBronzeDb = new glue.CfnDatabase(this, 'GlueDatabaseSalesBronze', {
            catalogId: this.account,
            databaseInput: {
                name: 'sales_bronze',
                description: 'Sales Bronze Database'
            }
        });

        // Glue Database - Silver
        const salesSilverDb = new glue.CfnDatabase(this, 'GlueDatabaseSalesSilver', {
            catalogId: this.account,
            databaseInput: {
                name: 'sales_silver',
                description: 'Sales Silver Database'
            }
        });

        // Glue Database - Gold
        const salesGoldDb = new glue.CfnDatabase(this, 'GlueDatabaseSalesGold', {
            catalogId: this.account,
            databaseInput: {
                name: 'sales_gold',
                description: 'Sales Gold Database'
            }
        });

        // ********************************************
        // RESOURCES EXPOSITION
        // ********************************************

        // Buckets
        this.bronzeBucketName = bronzeBucket.bucketName;
        this.silverBucketName = silverBucket.bucketName;
        this.goldBucketName = goldBucket.bucketName;
        this.resourcesBucketName = resourcesBucket.bucketName;

        // Glue Databases
        this.salesBronzeDbName = salesBronzeDb.ref;
        this.salesSilverDbName = salesSilverDb.ref;
        this.salesGoldDbName = salesGoldDb.ref;

        // ********************************************
        // OUTPUTS
        // ********************************************

        // Output the S3 Buckets for external reference
        new cdk.CfnOutput(this, 'S3BucketBronzeName', {
            value: bronzeBucket.bucketName,
            description: 'S3 Bucket Bronze Name'
        });

        new cdk.CfnOutput(this, 'S3BucketSilverName', {
            value: silverBucket.bucketName,
            description: 'S3 Bucket Silver Name'
        });

        new cdk.CfnOutput(this, 'S3BucketGoldName', {
            value: goldBucket.bucketName,
            description: 'S3 Bucket Gold Name'
        });

        new cdk.CfnOutput(this, 'S3BucketResourcesName', {
            value: resourcesBucket.bucketName,
            description: 'S3 Bucket Resources Name'
        });

        // Output the Glue Dabatases for external reference
        new cdk.CfnOutput(this, 'GlueDbSalesBronzeName', {
            value: salesBronzeDb.ref,
            description: 'Glue Database Sales Bronze Name'
        });

        new cdk.CfnOutput(this, 'GlueDbSalesSilverName', {
            value: salesSilverDb.ref,
            description: 'Glue Database Sales Silver Name'
        });

        new cdk.CfnOutput(this, 'GlueDbSalesGoldName', {
            value: salesGoldDb.ref,
            description: 'Glue Database Sales Gold Name'
        });
    }
}