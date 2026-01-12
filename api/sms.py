"""Fast Message SMS integration for payment notifications.

Uses API Key + Partner ID authentication (primary method).
Alternative: App Key + App Token authentication (if configured).
No token fetching required - credentials sent directly in request body.
"""

import requests
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Fast Message Configuration - Primary Method: API Key + Partner ID
FASTMESSAGE_API_KEY = os.environ.get('FASTMESSAGE_API_KEY', '')
FASTMESSAGE_PARTNER_ID = os.environ.get('FASTMESSAGE_PARTNER_ID', '')
FASTMESSAGE_SHORTCODE = os.environ.get('FASTMESSAGE_SHORTCODE', 'Daraja')

# Fast Message Configuration - Alternative Method: App Key + App Token
FASTMESSAGE_APP_KEY = os.environ.get('FASTMESSAGE_APP_KEY', '')
FASTMESSAGE_APP_TOKEN = os.environ.get('FASTMESSAGE_APP_TOKEN', '')

FASTMESSAGE_SMS_URL = "https://sms.fastmessage.co.ke/api/services/sendsms"

SMS_TIMEOUT = 15


def send_sms(phone: str, message: str) -> bool:
    """Send SMS via Fast Message using configured authentication.
    
    Primary method: API Key + Partner ID
    Fallback: App Key + App Token (if configured)
    
    Args:
        phone: Recipient phone number (with or without +254 prefix)
        message: SMS message text (GSM7 encoded)
        
    Returns:
        True if SMS was sent successfully, False otherwise
    """
    if not phone or not message:
        logger.warning('Missing phone or message for SMS')
        return False
    
    # Determine which authentication method to use
    use_api_key_auth = bool(FASTMESSAGE_API_KEY and FASTMESSAGE_PARTNER_ID)
    use_app_auth = bool(FASTMESSAGE_APP_KEY and FASTMESSAGE_APP_TOKEN)
    
    if not use_api_key_auth and not use_app_auth:
        logger.error(
            'Fast Message credentials not configured. '
            'Set either (FASTMESSAGE_API_KEY + FASTMESSAGE_PARTNER_ID) '
            'or (FASTMESSAGE_APP_KEY + FASTMESSAGE_APP_TOKEN)'
        )
        return False
    
    # Normalize phone number format (international format)
    normalized_phone = _normalize_phone_for_sms(phone)
    if not normalized_phone:
        logger.warning('Invalid phone number: %s', phone)
        return False
    
    # Build payload based on authentication method
    if use_api_key_auth:
        payload = {
            "apikey": FASTMESSAGE_API_KEY,
            "partnerID": FASTMESSAGE_PARTNER_ID,
            "message": message,
            "shortcode": FASTMESSAGE_SHORTCODE,
            "mobile": normalized_phone
        }
        auth_method = "API Key"
    else:
        # App Key + App Token authentication
        payload = {
            "appkey": FASTMESSAGE_APP_KEY,
            "apptoken": FASTMESSAGE_APP_TOKEN,
            "message": message,
            "shortcode": FASTMESSAGE_SHORTCODE,
            "mobile": normalized_phone
        }
        auth_method = "App Key + Token"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        logger.debug('Sending SMS to %s via Fast Message (%s)', normalized_phone, auth_method)
        response = requests.post(
            FASTMESSAGE_SMS_URL,
            json=payload,
            headers=headers,
            timeout=SMS_TIMEOUT
        )
        
        response_data = response.json()
        
        # Fast Message returns a responses array for POST requests
        if response.status_code == 200 and response_data.get('responses'):
            response_item = response_data['responses'][0]
            if response_item.get('response-code') == 200:
                logger.info('SMS sent successfully to %s (Message ID: %s)', 
                           normalized_phone, response_item.get('messageid'))
                return True
            else:
                logger.error(
                    'SMS send failed for %s (code: %s): %s',
                    normalized_phone,
                    response_item.get('response-code'),
                    response_item.get('response-description')
                )
                return False
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
    except (ValueError, KeyError) as e:
        logger.error('Invalid response format from Fast Message for %s: %s', normalized_phone, str(e))
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



