import os
import time
import logging
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from threading import Thread

from .config import (
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_SHEET_ID,
    ACCOUNTS_CACHE_TTL,
    PREDETERMINED_ACCOUNTS_ENV,
    DEBUG_SHEETS,
)

logger = logging.getLogger(__name__)

# Simple in-memory cache
_cache = {
    'accounts': None,
    'fetched_at': 0,
}

# Default predetermined accounts; can be overridden by PREDETERMINED_ACCOUNTS env var
FALLBACK_ACCOUNTS = [
    '600000',  # Sandbox ShortCode
    '600001',
    '600002',
    '600003',  # Additional test account
    'TEST001',
    'TEST002',
    'ACC004',  # Test account from simulation
]


def _get_service(write: bool = False):
    # Use centralized config
    keyfile = GOOGLE_SERVICE_ACCOUNT_FILE

    if not os.path.exists(keyfile):
        raise RuntimeError(f'Google service account file not found: {keyfile!s}')

    scopes = ['https://www.googleapis.com/auth/spreadsheets'] if write else ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes)
    return build('sheets', 'v4', credentials=creds)


def get_predetermined_accounts() -> List[str]:
    """Return the predetermined account list from env or fallback.

    The env var PREDETERMINED_ACCOUNTS may contain a comma-separated list.
    """
    if PREDETERMINED_ACCOUNTS_ENV:
        accounts = [a.strip() for a in PREDETERMINED_ACCOUNTS_ENV.split(',') if a.strip()]
        if accounts:
            return accounts
    return FALLBACK_ACCOUNTS


def is_valid_account(account_number: str) -> bool:
    if not account_number:
        return False
    accounts = get_predetermined_accounts()
    return str(account_number) in accounts


def _sanitize_sheet_name(name: str) -> str:
    return name.replace('\\', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')


def _ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str):
    # Check existing sheets
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets.properties').execute()
    sheets = meta.get('sheets', [])
    names = [s.get('properties', {}).get('title') for s in sheets]
    if sheet_name in names:
        return True
    # Add sheet
    requests_body = {
        'requests': [
            {'addSheet': {'properties': {'title': sheet_name}}}
        ]
    }
    try:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests_body).execute()
        return True
    except Exception:
        logger.exception('Failed to create sheet %s in spreadsheet %s', sheet_name, spreadsheet_id)
        return False


def write_payment_to_sheet(payment: Dict[str, Any], spreadsheet_id: str = None):
    """Write a payment row to the spreadsheet. This is synchronous; use the async wrapper to fire-and-forget.

    payment should contain: transId, time, amount, name, phone, accountNumber
    """
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEET_ID
    if not spreadsheet_id:
        logger.warning('No GOOGLE_SHEET_ID configured; skipping sheet write')
        return False

    try:
        logger.debug('Attempting to initialize Google Sheets service for write')
        service = _get_service(write=True)
        logger.debug('Google Sheets service initialized successfully')
    except Exception as e:
        logger.error('Failed to initialize Google Sheets service: %s', e, exc_info=True)
        return False

    safe_account = _sanitize_sheet_name(str(payment.get('accountNumber') or 'unknown'))
    logger.debug('Sanitized account name: %s', safe_account)
    
    # Ensure sheet exists
    if not _ensure_sheet_exists(service, spreadsheet_id, safe_account):
        logger.error('Failed to ensure sheet %s exists', safe_account)

    row = [
        payment.get('transId') or '',
        payment.get('time') or '',
        payment.get('amount') or '',
        payment.get('name') or '',
        payment.get('phone') or ''
    ]

    range_name = f"{safe_account}!A:E"
    body = {'values': [row]}
    try:
        logger.debug('Appending row to sheet %s: %s', safe_account, row)
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logger.info('Successfully wrote payment %s to sheet %s (account: %s)', payment.get('transId'), spreadsheet_id, safe_account)
        return True
    except Exception as e:
        logger.error('Failed to append payment to sheet %s: %s', safe_account, e, exc_info=True)
        return False


def write_payment_async(payment: Dict[str, Any], spreadsheet_id: str = None):
    """Fire-and-forget write to Google Sheets using a background thread."""
    def _worker():
        try:
            write_payment_to_sheet(payment, spreadsheet_id=spreadsheet_id)
        except Exception:
            logger.exception('Background sheet write failed for payment %s', payment.get('transId'))

    t = Thread(target=_worker, daemon=True)
    t.start()
    return True


def clear_cache():
    """Clear the accounts cache (useful for testing)."""
    _cache['accounts'] = None
    _cache['fetched_at'] = 0
