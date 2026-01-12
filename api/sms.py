"""Onfon Media SMS integration for payment notifications.

Uses API Key + Client ID authentication (recommended method).
No token fetching required - credentials sent directly in headers.
"""

import requests
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Onfon Media Configuration - API Key Authentication
ONFON_API_KEY = os.environ.get('ONFON_API_KEY', '')
ONFON_CLIENT_ID = os.environ.get('ONFON_CLIENT_ID', '')
ONFON_SENDER = os.environ.get('ONFON_SENDER', 'Daraja')

ONFON_SMS_URL = "https://api.onfonmedia.co.ke/v1/sms/send"

SMS_TIMEOUT = 15


def send_sms(phone: str, message: str) -> bool:
    """Send SMS via Onfon Media using API Key authentication.
    
    Args:
        phone: Recipient phone number (with or without +254 prefix)
        message: SMS message text
        
    Returns:
        True if SMS was sent successfully, False otherwise
    """
    if not phone or not message:
        logger.warning('Missing phone or message for SMS')
        return False
    
    # Validate credentials are configured
    if not ONFON_API_KEY or not ONFON_CLIENT_ID:
        logger.error('Onfon credentials not configured (ONFON_API_KEY and ONFON_CLIENT_ID required)')
        return False
    
    # Normalize phone number format (international format)
    normalized_phone = _normalize_phone_for_sms(phone)
    if not normalized_phone:
        logger.warning('Invalid phone number: %s', phone)
        return False
    
    payload = {
        "to": normalized_phone,
        "message": message,
        "sender": ONFON_SENDER
    }
    
    headers = {
        "Content-Type": "application/json",
        "Api-Key": ONFON_API_KEY,
        "Client-Id": ONFON_CLIENT_ID
    }
    
    try:
        logger.debug('Sending SMS to %s', normalized_phone)
        response = requests.post(
            ONFON_SMS_URL,
            json=payload,
            headers=headers,
            timeout=SMS_TIMEOUT
        )
        
        if response.status_code == 200:
            logger.info('SMS sent successfully to %s', normalized_phone)
            return True
        else:
            logger.error(
                'SMS send failed for %s (status: %d): %s',
                normalized_phone,
                response.status_code,
                response.text
            )
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error('Request failed while sending SMS to %s: %s', normalized_phone, str(e))
        return False
    except Exception as e:
        logger.exception('Unexpected error sending SMS to %s: %s', normalized_phone, str(e))
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



