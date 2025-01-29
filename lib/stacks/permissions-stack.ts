import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';

interface PermissionsStackProps extends cdk.NestedStackProps {
    bronzeBucketName: string;
    silverBucketName: string;
    goldBucketName: string;
    resourcesBucketName: string;
}

export class PermissionsStack extends cdk.NestedStack {
    public readonly glueCrawlerRoleArn: string;
    public readonly glueJobRoleArn: string;

    constructor(scope: Construct, id: string, props: PermissionsStackProps) {
        super(scope, id, props);

        // ********************************************
        // GLUE CRAWLERS ROLE
        // ********************************************

        // Create a Glue Crawler Role:
        const glueCrawlerRole = new iam.Role(this, 'GlueCrawlerRole', {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            roleName: 'Lab-Glue-Workshop-Crawler-Role',
            description: 'Role for Glue Crawlers',
        });

        // Create a Glue Crawler Policy:
        new iam.Policy(this, 'GlueCrawlerPolicy', {
            policyName: 'Lab-Glue-Workshop-Crawler-Policy',
            roles: [glueCrawlerRole],
            statements: [
                new iam.PolicyStatement({
                    sid: 'S3Permissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        's3:GetObject',
                        's3:ListBucket',
                        's3:GetBucketLocation'
                    ],
                    resources: [
                        `arn:aws:s3:::${props.bronzeBucketName}`,
                        `arn:aws:s3:::${props.bronzeBucketName}/*`,
                        `arn:aws:s3:::${props.silverBucketName}`,
                        `arn:aws:s3:::${props.silverBucketName}/*`,
                        `arn:aws:s3:::${props.goldBucketName}`,
                        `arn:aws:s3:::${props.goldBucketName}/*`
                    ]
                }),
                new iam.PolicyStatement({
                    sid: 'GluePermissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        'glue:GetDatabase',
                        'glue:GetTable',
                        'glue:UpdateTable',
                        'glue:CreateTable',
                        'glue:GetTables',
                        'glue:BatchCreatePartition',
                        'glue:BatchDeletePartition',
                        'glue:BatchGetPartition'
                    ],
                    resources: [
                        '*'
                    ]
                }),
                new iam.PolicyStatement({
                    sid: 'CloudwatchPermissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents'
                    ],
                    resources: [
                        '*'
                    ]
                })
            ]
        });

        // ********************************************
        // GLUE JOBS ROLE
        // ********************************************

        // Create a Glue Job Role:
        const glueJobRole = new iam.Role(this, 'GlueJobRole', {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            roleName: 'Lab-Glue-Workshop-Job-Role',
            description: 'Role for Glue Jobs',
        });

        // Create a Glue Job Policy:
        new iam.Policy(this, 'GlueJobPolicy', {
            policyName: 'Lab-Glue-Workshop-Job-Policy',
            roles: [glueJobRole],
            statements: [
                new iam.PolicyStatement({
                    sid: 'S3Permissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        's3:GetObject',
                        's3:ListBucket',
                        's3:GetBucketLocation',
                        's3:PutObject'
                    ],
                    resources: [
                        `arn:aws:s3:::${props.bronzeBucketName}`,
                        `arn:aws:s3:::${props.bronzeBucketName}/*`,
                        `arn:aws:s3:::${props.silverBucketName}`,
                        `arn:aws:s3:::${props.silverBucketName}/*`,
                        `arn:aws:s3:::${props.goldBucketName}`,
                        `arn:aws:s3:::${props.goldBucketName}/*`,
                        `arn:aws:s3:::${props.resourcesBucketName}`,
                        `arn:aws:s3:::${props.resourcesBucketName}/*`
                    ]
                }),
                new iam.PolicyStatement({
                    sid: 'GluePermissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        'glue:GetTable',
                        'glue:GetTables',
                        'glue:GetDatabase',
                        'glue:GetDatabases',
                        'glue:GetTableVersion',
                        'glue:GetTableVersions',
                        'glue:GetPartitions'
                    ],
                    resources: [
                        '*'
                    ]
                }),
                new iam.PolicyStatement({
                    sid: 'CloudwatchPermissions',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents',
                        'cloudwatch:PutMetricData'
                    ],
                    resources: [
                        '*'
                    ]
                })
            ]
        });

        // ********************************************
        // RESOURCES EXPOSITION
        // ********************************************

        this.glueCrawlerRoleArn = glueCrawlerRole.roleArn;
        this.glueJobRoleArn = glueJobRole.roleArn;

        // ********************************************
        // OUTPUTS
        // ********************************************

        new cdk.CfnOutput(this, 'GlueCrawlerRoleArn', {
            value: glueCrawlerRole.roleArn,
            description: 'Role ARN for Glue Crawlers'
        });

        new cdk.CfnOutput(this, 'GlueJobRoleArn', {
            value: glueJobRole.roleArn,
            description: 'Role ARN for Glue Jobs'
        });
    }
}