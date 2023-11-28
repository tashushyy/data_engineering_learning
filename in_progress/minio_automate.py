import subprocess

class MinioClient:

    def __init__(self, endpoint, access_key=None, secret_key=None, region_name=None):
        """
        Initialize a new instance of the MinioClient class.

        :param endpoint: The endpoint URL of the Minio server
        :type endpoint: str
        :param access_key: The access key to use for authentication (optional)
        :type access_key: str
        :param secret_key: The secret key to use for authentication (optional)
        :type secret_key: str
        :param region_name: The AWS region name to use (optional)
        :type region_name: str
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key

    def create_bucket(self, bucket_name):
        """
        Create a new bucket.

        :param bucket_name: The name of the bucket to create
        :type bucket_name: str
        :return: None
        :raises Exception: If the bucket already exists or cannot be created
        """
        command = ['mc', 'mb', self.get_bucket_url(bucket_name)]
        result = self.execute_command(command)
        if result.returncode != 0:
            raise Exception('Failed to create bucket {}: {}'.format(bucket_name, result.stderr))

    def list_buckets(self):
        """
        List all buckets in the server.

        :return: A list of bucket names
        :rtype: list
        :raises Exception: If the list request fails
        """
        command = ['mc', 'ls']
        result = self.execute_command(command)
        if result.returncode != 0:
            raise Exception('Failed to list buckets: {}'.format(result.stderr))
        output = result.stdout.strip()
        if not output:
            return []
        buckets = [line.split()[1] for line in output.split('\n')]
        return buckets

    def delete_bucket(self, bucket_name):
        """
        Delete a bucket.

        :param bucket_name: The name of the bucket to delete
        :type bucket_name: str
        :return: None
        :raises Exception: If the bucket does not exist or cannot be deleted
        """
        command = ['mc', 'rb', self.get_bucket_url(bucket_name), '--force']
        result = self.execute_command(command)
        if result.returncode != 0:
            raise Exception('Failed to delete bucket {}: {}'.format(bucket_name, result.stderr))

    def execute_command(self, command):
        """
        Execute a MinIO CLI command and capture its output.

        :param command: The MinIO CLI command to execute
        :type command: list
        :return: The result of the command execution
        :rtype: subprocess.CompletedProcess
        """
        environment = {
            'MC_HOST': self.endpoint,
            'MC_ACCESS_KEY': self.access_key,
            'MC_SECRET_KEY': self.secret_key,
            'MC_REGION': self.region_name,
        }
        return subprocess.run(command, env=environment, capture_output=True, text=True)

    def get_bucket_url(self, bucket_name):
        """
        Get the URL of a bucket.

        :param bucket_name: The name of the bucket
        :type bucket_name: str
        :return: The URL of the bucket
        :rtype: str
        """
        return 's3/{}'.format(bucket_name)
