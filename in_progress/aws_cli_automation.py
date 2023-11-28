import subprocess

class AWSCLI:

    def __init__(self, region=None, profile=None):
        """
        Initialize a new instance of the AWSCLI class.

        :param region: The AWS region to use for CLI commands (optional)
        :type region: str
        :param profile: The AWS profile to use for CLI commands (optional)
        :type profile: str
        """
        self.region = region
        self.profile = profile

    def run(self, command):
        """
        Execute an AWS CLI command.

        :param command: The AWS CLI command to execute
        :type command: str
        :return: The output of the command
        :rtype: str
        :raises Exception: If the command returns a non-zero exit code
        """
        aws_command = ['aws']
        if self.region:
            aws_command += ['--region', self.region]
        if self.profile:
            aws_command += ['--profile', self.profile]
        aws_command += command.split()
        result = subprocess.run(aws_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(result.stderr.strip())
        return result.stdout.strip()

    def create_s3_bucket(self, bucket_name):
        """
        Create a new S3 bucket.

        :param bucket_name: The name of the bucket to create
        :type bucket_name: str
        :return: The output of the `aws s3api create-bucket` command
        :rtype: str
        """
        command = f's3api create-bucket --bucket {bucket_name}'
        return self.run(command)

    def list_s3_buckets(self):
        """
        List all S3 buckets in the account.

        :return: The output of the `aws s3api list-buckets` command
        :rtype: str
        """
        command = 's3api list-buckets'
        return self.run(command)

    def delete_s3_bucket(self, bucket_name):
        """
        Delete an S3 bucket.

        :param bucket_name: The name of the bucket to delete
        :type bucket_name: str
        :return: The output of the `aws s3api delete-bucket` command
        :rtype: str
        """
        command = f's3api delete-bucket --bucket {bucket_name}'
        return self.run(command)
