import unittest
from unittest.mock import Mock, patch

from api import sms


class TestSmsIntegration(unittest.TestCase):
    def test_send_sms_success_uses_talksasa_payload_and_headers(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'status': 'success'}

        with patch.object(sms, 'TALKSASA_API_TOKEN', 'test-token'):
            with patch.object(sms, 'TALKSASA_SENDER_ID', 'YourName'):
                with patch.object(sms, 'TALKSASA_SMS_URL', 'https://bulksms.talksasa.com/api/v3/sms/send'):
                    with patch('api.sms.requests.post', return_value=mock_response) as mock_post:
                        result = sms.send_sms('0712345678', 'This is a test message')

        self.assertTrue(result)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], 'https://bulksms.talksasa.com/api/v3/sms/send')
        self.assertEqual(kwargs['json'], {
            'recipient': '254712345678',
            'sender_id': 'YourName',
            'type': 'plain',
            'message': 'This is a test message'
        })
        self.assertEqual(kwargs['headers'], {
            'Authorization': 'Bearer test-token',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def test_send_sms_returns_false_when_credentials_missing(self):
        with patch.object(sms, 'TALKSASA_API_TOKEN', ''):
            with patch.object(sms, 'TALKSASA_SENDER_ID', ''):
                result = sms.send_sms('0712345678', 'This is a test message')

        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
