#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { RootStack } from '../lib/stacks/root-stack';

const app = new cdk.App();

new RootStack(app, 'EtlGlueWorkshopStack', {
  description: 'Main stack for the ETL Glue Workshop',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
});