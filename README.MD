# aws-s3-orphan-parts-cleanup

## Overview

When using AWS S3 multipart upload, if the client fails to complete or abort an upload in progress, the parts that has already been uploaded will remain in the bucket. The parts are invisible from the AWS console, and only accessible through ListMultipartUpload and ListParts APIs. Moreover, the parts cannot be deleted through the console, and the parts are not affected even when auto-delete is enabled in the bucket.

For now, the recommended solution is to manually abort the parts in progress, and this is a sample code to do that.

## Usage

> Make sure whatever credentials you are using, it has access to s3:ListMultipartUpload, s3:ListParts, and s3:AbortMultipartUpload to the specified bucket.

1. Construct a new instance of CleanupService by passing in your AmazonS3 instance.
2. CleanupService.cleanup(s3BucketName) to initiate a cleanup for the specified bucket name.

The supplied code by default will pick up AWS credentials, and proxy settings from a .properties file located at the root of the project when autowired using Spring context.

## Running the test

1. Configure the supplied '.properties' file with the AWS credentials, proxy settings, and test bucket name.
2. Execute CleanupServiceTest.java as a JUnit test.