import logging

import ibm_boto3
import time
from ibm_botocore.config import Config
from ibm_botocore.exceptions import ClientError


class COS(object):
    """Interface to BlueMix Object Storage
    gets params from Recipe:
      {
        "COS": {
            "credentials": "../bluemix/cos_credentials",
            "endpoint": "https://s3.eu-de.objectstorage.softlayer.net",
            "bucket": "cog-1"
        }
      }
    """
    log = logging.getLogger('COS')

    # log = None  # type: Logger

    def __init__(self, recipe):
        """

        :type recipe: Recipe
        """

        # self.log.setLevel(DEBUG)
        self.log.info("Connection to COS")
        try:
            cos_creds = recipe.cos_creds_content()
        except AssertionError as e:
            self.log.warning(f"COS credentials are required....{e}")
            raise AssertionError(f'COS credentials are required: {e}')
        type = recipe['COS']["type"] if 'type' in recipe['COS'] else 'IBM'
        if type == 'IBM':
            self.log.error('config')
            cos_creds['config'] = Config(signature_version='oauth')
        # api_key = cos_creds['apikey']
        # auth_endpoint = 'https://iam.bluemix.net/oidc/token'
        # service_instance_id = cos_creds['resource_instance_id']
        service_endpoint = recipe["COS"]["endpoint"]
        # service_endpoint = "s3.eu-de.objectstorage.service.networklayer.com"
        self.bucket = recipe["COS"]["bucket"]
        self.log.info("service endpoint '%s'", service_endpoint)
        self.log.info("service bucket '%s'", self.bucket)
        try:
            self.resource = ibm_boto3.resource(
                's3',
                # ibm_api_key_id=cos_creds["ibm_api_key_id"],
                # ibm_service_instance_id=cos_creds["ibm_service_instance_id"],
                # ibm_auth_endpoint=cos_creds["ibm_auth_endpoint"],
                # config=Config(signature_version='oauth'),
                endpoint_url=service_endpoint,
                **cos_creds,
            )
        except ClientError as e:
            self.log.fatal('Exception: %s', e)
            raise SystemExit(-1)

    def create_bucket(self):
        bucket = self.resource.Bucket(self.bucket)
        bucket.create(ACL='public-read',LocationConstraint="eu-central-1")
        bucket.wait_until_exists()
        self._update_CORS()

    def check_bucket(self):
        # TODO got error when try to re-create DELETED Bucketibm_botocore.errorfactory.BucketAlreadyExists: An error occurred (BucketAlreadyExists) when calling the CreateBucket operation: Container cog-ttt exists
        try:
            if self.resource.Bucket(self.bucket).creation_date is None:
                self.create_bucket()
            else:
                self.log.debug(f"Bucket '{self.bucket}' Exists!")
                self._update_CORS()
            return True
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.warning(f"Private {self.bucket} Bucket. Forbidden Access!")
                return True
            elif error_code == 404:
                self.log.info(f"Bucket  {self.bucket}  Does Not Exist!")
                try:
                    self.create_bucket()
                    return True
                except ClientError as e:
                    self.log.fatal('Exception: %s', e)
            return False

    def _update_CORS(self):
        cors_configuration = {
            'CORSRules': [{
                'AllowedHeaders': ['Authorization'],
                'AllowedMethods': ['GET', 'HEAD'],
                'AllowedOrigins': ['*'],
                'ExposeHeaders': ['GET', 'HEAD', 'OPTIONS'],
                'MaxAgeSeconds': 3000
            }]
        }
        self.resource.BucketCors(self.bucket).put(CORSConfiguration=cors_configuration)

    def publish(self, file, item_name):
        """        upload <file> to COS into bucket from Recipe
         as <item_name>(key) and make it public-read

        :param file: str
        :param item_name: str
        :return: None
        """

        start = time.time()
        try:
            self.check_bucket()
            self.resource.Bucket(self.bucket).upload_file(file, item_name, ExtraArgs={'ACL': 'public-read'})
            self.log.info(f"file '{file}'uploaded to bucket '{self.bucket}' as '{item_name}' in %s sec",
                          time.time() - start)
        except ClientError as e:
            self.log.fatal(f"Could upload {file} as {item_name} into {self.bucket}")
