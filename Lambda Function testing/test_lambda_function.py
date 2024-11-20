import unittest
from unittest.mock import patch, MagicMock
import os
from lambda_function import lambda_handler  # Replace with your actual file name if needed

class TestLambdaHandler(unittest.TestCase):

    @patch('boto3.client')  # Mock boto3.client to prevent actual S3 calls
    def test_lambda_handler_success(self, mock_boto_client):
        # Create a mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock list_objects_v2 response for source bucket
        mock_s3_client.list_objects_v2.return_value = {
            'CommonPrefixes': [{'Prefix': 'pollution/Kyiv/'}, {'Prefix': 'pollution/Other/'}],
            'Contents': [{'Key': 'pollution/Kyiv/file1.csv'}, {'Key': 'pollution/Kyiv/file2.csv'}]
        }

        # Mock the copy_object to avoid real S3 interaction
        mock_s3_client.copy_object = MagicMock()

        # Set environment variables for source and destination buckets
        os.environ['SOURCE_BUCKET'] = 'source-bucket'
        os.environ['DESTINATION_BUCKET'] = 'destination-bucket'

        # Sample event and context
        sample_event = {}
        sample_context = {}

        # Call the lambda_handler function
        response = lambda_handler(sample_event, sample_context)

        # Assert that list_objects_v2 was called with the correct parameters
        mock_s3_client.list_objects_v2.assert_called_with(Bucket='source-bucket', Prefix='pollution/Kyiv/')

        # Assert that copy_object was called for each file in the 'Kyiv' prefix
        mock_s3_client.copy_object.assert_any_call(
            CopySource={'Bucket': 'source-bucket', 'Key': 'pollution/Kyiv/file1.csv'},
            Bucket='destination-bucket',
            Key='pollution/Kyiv/file1.csv'
        )
        mock_s3_client.copy_object.assert_any_call(
            CopySource={'Bucket': 'source-bucket', 'Key': 'pollution/Kyiv/file2.csv'},
            Bucket='destination-bucket',
            Key='pollution/Kyiv/file2.csv'
        )

        # Assert that the final response contains the expected statusCode
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Transferred files', response['body'])

    @patch('boto3.client')  # Mock boto3.client to test no matching prefix scenario
    def test_lambda_handler_no_matching_prefix(self, mock_boto_client):
        # Create a mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock list_objects_v2 response with no matching prefixes
        mock_s3_client.list_objects_v2.return_value = {
            'CommonPrefixes': [{'Prefix': 'pollution/Other/'}],
            'Contents': []
        }

        # Mock the copy_object to avoid real S3 interaction
        mock_s3_client.copy_object = MagicMock()

        # Set environment variables for source and destination buckets
        os.environ['SOURCE_BUCKET'] = 'source-bucket'
        os.environ['DESTINATION_BUCKET'] = 'destination-bucket'

        # Sample event and context
        sample_event = {}
        sample_context = {}

        # Call the lambda_handler function
        response = lambda_handler(sample_event, sample_context)

        # Assert that list_objects_v2 was called with the correct parameters
        mock_s3_client.list_objects_v2.assert_called_with(Bucket='source-bucket', Prefix='pollution/', Delimiter='/')

        # Ensure that copy_object was not called since no matching files were found
        mock_s3_client.copy_object.assert_not_called()

        # Assert that the response contains the expected message
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Transferred files', response['body'])

if __name__ == "__main__":
    unittest.main()
