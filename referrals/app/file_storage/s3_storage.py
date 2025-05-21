# monolith/referrals/app/file_storage/s3_storage.py
"""
S3 storage functionality for uploading and retrieving files.
"""
import boto3
import os
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class S3Storage:
    def __init__(self, bucket_name=None, region=None):
        """Initialize S3 storage with bucket name and region."""
        self.bucket_name = bucket_name or os.environ.get('S3_BUCKET_NAME')
        self.region = region or os.environ.get('AWS_REGION', 'us-east-1')
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=self.region
        )
    
    def upload_file(self, file_data, key, content_type=None):
        """
        Upload file to S3 bucket.
        
        Args:
            file_data: Binary file data to upload
            key: S3 object key (path/filename.ext)
            content_type: MIME type of the file
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.put_object(
                Body=file_data,
                Bucket=self.bucket_name,
                Key=key,
                **extra_args
            )
            
            logger.info(f"Successfully uploaded file to s3://{self.bucket_name}/{key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            return False
    
    def download_file(self, key):
        """
        Download file from S3 bucket.
        
        Args:
            key: S3 object key
            
        Returns:
            bytes: File data if successful, None otherwise
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
            
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {str(e)}")
            return None
    
    def generate_presigned_url(self, key, expiration=3600):
        """
        Generate a presigned URL for file download.
        
        Args:
            key: S3 object key
            expiration: URL expiration in seconds (default: 1 hour)
            
        Returns:
            str: Presigned URL if successful, None otherwise
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': key},
                ExpiresIn=expiration
            )
            return url
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None