"""Talksasa SMS integration for payment notifications.

Uses Talksasa bearer token authentication and sender ID.
"""

import requests
import logging
import os
from typing import Optional
from utils.error_tracker import log_transaction_error, ErrorSource

logger = logging.getLogger(__name__)

# Talksasa configuration
TALKSASA_API_TOKEN = os.environ.get('TALKSASA_API_TOKEN', '')
TALKSASA_SENDER_ID = os.environ.get('TALKSASA_SENDER_ID', '')
TALKSASA_BASE_URL = os.environ.get('TALKSASA_BASE_URL', 'https://bulksms.talksasa.com/api/v3')
TALKSASA_SMS_URL = f"{TALKSASA_BASE_URL.rstrip('/')}/sms/send"

SMS_TIMEOUT = 15


def send_sms(phone: str, message: str) -> bool:
    """Send SMS via Talksasa using configured bearer token authentication.
    
    Args:
        phone: Recipient phone number (with or without +254 prefix)
        message: SMS message text
        
    Returns:
        True if SMS was sent successfully, False otherwise
    """
    if not phone or not message:
        logger.warning('Missing phone or message for SMS')
        return False

    if not TALKSASA_API_TOKEN or not TALKSASA_SENDER_ID:
        logger.error(
            'Talksasa SMS credentials not configured. '
            'Set TALKSASA_API_TOKEN and TALKSASA_SENDER_ID.'
        )
        log_transaction_error(
            error_source=ErrorSource.API_KEYS,
            error_message="Talksasa SMS credentials not configured",
            context={'method': 'bearer_token_auth'}
        )
        return False

    # Normalize phone number format (international format)
    normalized_phone = _normalize_phone_for_sms(phone)
    if not normalized_phone:
        logger.warning('Invalid phone number: %s', phone)
        log_transaction_error(
            error_source=ErrorSource.VALIDATION,
            error_message="Invalid phone number format for SMS",
            context={'phone': phone}
        )
        return False

    payload = {
        "recipient": normalized_phone,
        "sender_id": TALKSASA_SENDER_ID,
        "type": "plain",
        "message": message
    }

    headers = {
        "Authorization": f"Bearer {TALKSASA_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        logger.debug('Sending SMS to %s via Talksasa', normalized_phone)
        response = requests.post(
            TALKSASA_SMS_URL,
            json=payload,
            headers=headers,
            timeout=SMS_TIMEOUT
        )

        response_data = response.json()
        
        success = (
            response.status_code == 200 and
            (
                response_data.get('status') == 'success' or
                response_data.get('success') is True or
                not response_data.get('errors')
            )
        )

        if success:
            logger.info('SMS sent successfully to %s', normalized_phone)
            return True

        error_desc = response_data.get('message') or response_data.get('errors') or response.text
        logger.error(
            'SMS send failed for %s (status: %d): %s',
            normalized_phone,
            response.status_code,
            error_desc
        )
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message=f"SMS delivery failed - {error_desc}",
            context={
                'phone': normalized_phone,
                'status_code': response.status_code,
                'response': response.text[:200]
            }
        )
        return False

    except requests.exceptions.ConnectionError as e:
        logger.error('Connection failed while sending SMS to %s: %s', normalized_phone, str(e))
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message="SMS gateway connection failed",
            exception=e,
            context={'phone': normalized_phone}
        )
        return False
    except requests.exceptions.Timeout as e:
        logger.error('Request timeout while sending SMS to %s: %s', normalized_phone, str(e))
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message="SMS gateway request timeout",
            exception=e,
            context={'phone': normalized_phone}
        )
        return False
    except requests.exceptions.RequestException as e:
        logger.error('Request failed while sending SMS to %s: %s', normalized_phone, str(e))
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message="SMS gateway request failed",
            exception=e,
            context={'phone': normalized_phone}
        )
        return False
    except (ValueError, KeyError) as e:
        logger.error('Invalid response format from Talksasa for %s: %s', normalized_phone, str(e))
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message="SMS gateway returned invalid response format",
            exception=e,
            context={'phone': normalized_phone}
        )
        return False
    except Exception as e:
        logger.exception('Unexpected error sending SMS to %s: %s', normalized_phone, str(e))
        log_transaction_error(
            error_source=ErrorSource.SMS_GATEWAY,
            error_message="Unexpected error during SMS delivery",
            exception=e,
            context={'phone': normalized_phone}
        )
        return False


def _normalize_phone_for_sms(phone: str) -> str:
    """Normalize phone number to international format (254...).
    
    Converts local format (07..., 0...) to international format (254...).
    
    Args:
        phone: Phone number in any format
        
    Returns:
        Normalized phone in international format, or empty string if invalid
    """
    if not phone:
        return ''
    
    # Remove all non-digit characters except leading +
    normalized = ''.join(c for c in str(phone).strip() if c.isdigit() or c == '+')
    
    if not normalized:
        return ''
    
    # Remove leading + if present
    if normalized.startswith('+'):
        normalized = normalized[1:]
    
    # If starts with 0 (local format), replace with 254
    if normalized.startswith('0'):
        normalized = '254' + normalized[1:]
    # If doesn't start with 254, assume local and prepend 254
    elif not normalized.startswith('254'):
        # If it starts with a digit but not 0 or 254, prepend 254
        normalized = '254' + normalized
    
    return normalized



