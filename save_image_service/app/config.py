import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    YDB_ENDPOINT = os.getenv('YDB_ENDPOINT')
    YDB_DATABASE = os.getenv('YDB_DATABASE')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    YC_IAM_TOKEN = os.getenv('YC_IAM_TOKEN')
    OAUTH_TOKEN = os.getenv('OAUTH_TOKEN')
    SERVICE_ACC_ID = os.getenv('SERVICE_ACC_ID')
    
    @classmethod
    def validate(cls):
        required_vars = [
            'YDB_ENDPOINT', 'YDB_DATABASE',
            'BUCKET_NAME', 'S3_ENDPOINT',
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'
        ]
        for var in required_vars:
            if not getattr(cls, var):
                raise ValueError(f'Missing required config variable: {var}')