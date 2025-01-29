import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';

interface TransformationStackProps extends cdk.NestedStackProps {
    bronzeBucketName: string;
    silverBucketName: string;
    goldBucketName: string;
    resourcesBucketName: string;
    salesBronzeDbName: string;
    salesSilverDbName: string;
    salesGoldDbName: string;
    glueCrawlerRoleArn: string;
    glueJobRoleArn: string;
}

export class TransformationStack extends cdk.NestedStack {
    constructor(scope: Construct, id: string, props: TransformationStackProps) {
        super(scope, id, props);

        // ********************************************
        // GLUE CLASSIFIERS
        // ********************************************

        const glueCsvClassifier = new glue.CfnClassifier(this, 'GlueCsvClassifier', {
            csvClassifier: {
                name: 'CsvClassifier',
                delimiter: ',',
                quoteSymbol: '"',
                containsHeader: 'PRESENT',
                allowSingleColumn: true,
                disableValueTrimming: true
            }
        });

        // ********************************************
        // GLUE CRAWLERS
        // ********************************************

        // Sales Crawler - Bronze
        const crawlerSalesBronze = new glue.CfnCrawler(this, 'CrawlerSalesBronze', {
            name: 'crwl_sales_bronze',
            description: 'Crawler for Sales Data in Bronze layer',
            targets: {
                s3Targets: [
                    { path: `s3://${props.bronzeBucketName}/customers/` },
                    { path: `s3://${props.bronzeBucketName}/order_items/` },
                    { path: `s3://${props.bronzeBucketName}/order_payments/` },
                    { path: `s3://${props.bronzeBucketName}/orders/` },
                    { path: `s3://${props.bronzeBucketName}/products/` }
                ]
            },
            recrawlPolicy: {
                recrawlBehavior: 'CRAWL_EVERYTHING'
            },
            classifiers: [
                glueCsvClassifier.ref
            ],
            role: props.glueCrawlerRoleArn,
            databaseName: props.salesBronzeDbName,
            schemaChangePolicy: {
                updateBehavior: 'UPDATE_IN_DATABASE',
                deleteBehavior: 'DELETE_FROM_DATABASE'
            }
        });

        // Sales Crawler - Silver
        const crawlerSalesSilver = new glue.CfnCrawler(this, 'CrawlerSalesSilver', {
            name: 'crwl_sales_silver',
            description: 'Crawler for Sales Data in Silver layer',
            targets: {
                s3Targets: [
                    { path: `s3://${props.silverBucketName}/customers/` },
                    { path: `s3://${props.silverBucketName}/order_items/` },
                    { path: `s3://${props.silverBucketName}/order_payments/` },
                    { path: `s3://${props.silverBucketName}/orders/` },
                    { path: `s3://${props.silverBucketName}/products/` }
                ]
            },
            recrawlPolicy: {
                recrawlBehavior: 'CRAWL_EVERYTHING'
            },
            role: props.glueCrawlerRoleArn,
            databaseName: props.salesSilverDbName,
            schemaChangePolicy: {
                updateBehavior: 'UPDATE_IN_DATABASE',
                deleteBehavior: 'DELETE_FROM_DATABASE'
            }
        });

        // Sales Crawler - Gold
        const crawlerSalesGold = new glue.CfnCrawler(this, 'CrawlerSalesGold', {
            name: 'crwl_sales_gold',
            description: 'Crawler for Sales Data in Gold layer',
            targets: {
                s3Targets: [
                    { path: `s3://${props.goldBucketName}/sales/` }
                ]
            },
            recrawlPolicy: {
                recrawlBehavior: 'CRAWL_EVERYTHING'
            },
            role: props.glueCrawlerRoleArn,
            databaseName: props.salesGoldDbName,
            schemaChangePolicy: {
                updateBehavior: 'UPDATE_IN_DATABASE',
                deleteBehavior: 'DELETE_FROM_DATABASE'
            }
        });

        // ********************************************
        // GLUE JOBS
        // ********************************************

        // Process Sales Bronze Data
        const jobProcessSalesBronzeConfig = {
            source_db: props.salesBronzeDbName,
            target_bucket: props.silverBucketName
        }
        
        const jobProcessSalesBronze = new glue.CfnJob(this, 'JobProcessSalesBronze', {
            name: 'Process-Sales-Bronze',
            description: 'Job for processing data located in Sales Bronze Layer',
            role: props.glueJobRoleArn,
            command: {
                name: 'glueetl',
                pythonVersion: '3',
                scriptLocation: `s3://${props.resourcesBucketName}/scripts/glue/process_sales_bronze.py`,
            },
            glueVersion: '5.0',
            workerType: 'G.1X',
            numberOfWorkers: 2,
            defaultArguments: {
                '--enable-auto-scaling': 'true',
                '--enable-job-insights': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--enable-metrics': 'true',
                '--enable-observability-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': `s3://${props.resourcesBucketName}/logs/sparkHistoryLogs/process_sales_bronze/`,
                '--TempDir': `s3://${props.resourcesBucketName}/temporary/`,
                '--enable-glue-datacatalog': 'true',
                '--SALES_PARAMS': JSON.stringify(jobProcessSalesBronzeConfig)
            },
            executionClass: 'FLEX',
            maxRetries: 0,
            timeout: 10,
            executionProperty: {
                maxConcurrentRuns: 1
            }
          });

          // Process Sales Silver Data
        const jobProcessSalesSilverConfig = {
            source_db: props.salesSilverDbName,
            target_bucket: props.goldBucketName
        }
        
        const jobProcessSalesSilver = new glue.CfnJob(this, 'JobProcessSalesSilver', {
            name: 'Process-Sales-Silver',
            description: 'Job for processing data located in Sales Silver Layer',
            role: props.glueJobRoleArn,
            command: {
                name: 'glueetl',
                pythonVersion: '3',
                scriptLocation: `s3://${props.resourcesBucketName}/scripts/glue/process_sales_silver.py`,
            },
            glueVersion: '5.0',
            workerType: 'G.1X',
            numberOfWorkers: 2,
            defaultArguments: {
                '--enable-auto-scaling': 'true',
                '--enable-job-insights': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--enable-metrics': 'true',
                '--enable-observability-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': `s3://${props.resourcesBucketName}/logs/sparkHistoryLogs/process_sales_silver/`,
                '--TempDir': `s3://${props.resourcesBucketName}/temporary/`,
                '--enable-glue-datacatalog': 'true',
                '--SALES_PARAMS': JSON.stringify(jobProcessSalesSilverConfig)
            },
            executionClass: 'FLEX',
            maxRetries: 0,
            timeout: 10,
            executionProperty: {
                maxConcurrentRuns: 1
            }
          });

        // ********************************************
        // OUTPUTS
        // ********************************************

        // Classifiers
        new cdk.CfnOutput(this, 'GlueCsvClassifierName', {
            value: glueCsvClassifier.ref,
            description: 'Glue CSV Classifier'
        });

        // Crawlers
        new cdk.CfnOutput(this, 'CrawlerSalesBronzeName', {
            value: crawlerSalesBronze.ref,
            description: 'Crawler Sales Bronze'
        });

        new cdk.CfnOutput(this, 'CrawlerSalesSilverName', {
            value: crawlerSalesSilver.ref,
            description: 'Crawler Sales Silver'
        });

        new cdk.CfnOutput(this, 'CrawlerSalesGoldName', {
            value: crawlerSalesGold.ref,
            description: 'Crawler Sales Gold'
        });

        // Jobs
        new cdk.CfnOutput(this, 'JobProcessSalesBronzeName', {
            value: jobProcessSalesBronze.ref,
            description: 'Job Process Sales Bronze'
        });

        new cdk.CfnOutput(this, 'JobProcessSalesSilverName', {
            value: jobProcessSalesSilver.ref,
            description: 'Job Process Sales Silver'
        });
    }
}