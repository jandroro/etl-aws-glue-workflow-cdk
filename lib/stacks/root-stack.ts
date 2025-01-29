import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { StorageStack } from './storage-stack';
import { PermissionsStack } from './permissions-stack';
import { TransformationStack } from './transformation-stack';

export class RootStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Storage Stack
        const storageStack = new StorageStack(this, 'StorageStack');

        // Permissions Stack
        const permissionsStack = new PermissionsStack(this, 'PermissionsStack', {
            bronzeBucketName: storageStack.bronzeBucketName,
            silverBucketName: storageStack.silverBucketName,
            goldBucketName: storageStack.goldBucketName,
            resourcesBucketName: storageStack.resourcesBucketName
        });
        
        // Transformation Stack
        const transformationStack = new TransformationStack(this, 'TransformationStack', {
            bronzeBucketName: storageStack.bronzeBucketName,
            silverBucketName: storageStack.silverBucketName,
            goldBucketName: storageStack.goldBucketName,
            resourcesBucketName: storageStack.resourcesBucketName,
            salesBronzeDbName: storageStack.salesBronzeDbName,
            salesSilverDbName: storageStack.salesSilverDbName,
            salesGoldDbName: storageStack.salesGoldDbName,
            glueCrawlerRoleArn: permissionsStack.glueCrawlerRoleArn,
            glueJobRoleArn: permissionsStack.glueJobRoleArn
        });
    }
}