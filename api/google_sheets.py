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

# Default predetermined accounts with Team Name and Team Phone Numbers
# Format: (AccountNumber, TeamName, [PhoneNumber1, PhoneNumber2, ...])
FALLBACK_ACCOUNTS = [
    ('600000', 'Sandbox Team', ['0723145610', '0723145611']),  # Sandbox ShortCode
    ('600001', 'Team One', ['0723145610', '0723145611']),
    ('600002', 'Team Two', ['0723145620', '0723145621']),
    ('600003', 'Team Three', ['0723145630', '0723145631']),  # Additional test account
    ('TEST001', 'Test Team One', ['0723145610']),
    ('TEST002', 'Test Team Two', ['0723145620']),
    ('ACC004', 'Team Four', ['0723145640', '0723145641']),  # Test account from simulation
    ('ACC001', 'Team One Alt', ['0723145610']),  # Test account from simulation
    ('ACC006', 'Team Six', ['0723145660', '0723145661']),  # Test account from simulation
    ('ACC002', 'Team Two Alt', ['0723145620']),  # Test account from simulation

    # New Account No's
    ('001', 'Team One', ['0723145610', '0723145611']),
    ('002', 'Team Two', ['0723145620', '0723145621']),
    ('003', 'Team Three', ['0723145630', '0723145631']),
    ('004', 'Team Four', ['0723145640', '0723145641']),
    ('005', 'Team Five', ['0723145650', '0723145651']),
    ('006', 'Team Six', ['0723145660', '0723145661']),
    ('007', 'Team Seven', ['0723145670', '0723145671']),
    ('008', 'Team Eight', ['0723145680', '0723145681']),
    ('009', 'Team Nine', ['0723145690', '0723145691']),
    ('010', 'Team Ten', ['0723145700', '0723145701']),
]


def _get_service(write: bool = False):
    # Use centralized config
    keyfile = GOOGLE_SERVICE_ACCOUNT_FILE

    if not os.path.exists(keyfile):
        raise RuntimeError(f'Google service account file not found: {keyfile!s}')

    scopes = ['https://www.googleapis.com/auth/spreadsheets'] if write else ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes)
    return build('sheets', 'v4', credentials=creds)


def get_predetermined_accounts() -> List[tuple]:
    """Return the predetermined account list from env or fallback.

    Returns list of tuples: (AccountNumber, TeamName, [PhoneNumbers])
    The env var PREDETERMINED_ACCOUNTS may contain a comma-separated list of account numbers.
    """
    if PREDETERMINED_ACCOUNTS_ENV:
        account_numbers = [a.strip() for a in PREDETERMINED_ACCOUNTS_ENV.split(',') if a.strip()]
        if account_numbers:
            # Filter FALLBACK_ACCOUNTS to only include those in PREDETERMINED_ACCOUNTS_ENV
            return [acc for acc in FALLBACK_ACCOUNTS if acc[0] in account_numbers]
    return FALLBACK_ACCOUNTS


def is_valid_account(account_number: str) -> bool:
    if not account_number:
        return False
    accounts = get_predetermined_accounts()
    account_numbers = [acc[0] for acc in accounts]
    return str(account_number) in account_numbers


def _sanitize_sheet_name(name: str) -> str:
    return name.replace('\\', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')


def normalize_phone(phone: str) -> str:
    """Normalize phone number by removing spaces, dashes, and other non-digit characters.
    
    Preserves the phone number digits and the leading '+' if present.
    """
    if not phone:
        return ''
    
    # Remove common separators and spaces, but keep digits and leading +
    normalized = ''.join(c for c in str(phone).strip() if c.isdigit() or c == '+')
    return normalized


def _ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> tuple:
    """Check if sheet exists; if not, create it. Returns (exists, is_new)."""
    # Check existing sheets
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets.properties').execute()
    sheets = meta.get('sheets', [])
    names = [s.get('properties', {}).get('title') for s in sheets]
    
    if sheet_name in names:
        return True, False
    
    # Sheet doesn't exist, create it
    requests_body = {
        'requests': [
            {'addSheet': {'properties': {'title': sheet_name}}}
        ]
    }
    try:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests_body).execute()
        return True, True  # Created successfully and is new
    except Exception:
        logger.exception('Failed to create sheet %s in spreadsheet %s', sheet_name, spreadsheet_id)
        return False, False


def write_payment_to_sheet(payment: Dict[str, Any], spreadsheet_id: str = None):
    """Write a payment row to the spreadsheet. This is synchronous; use the async wrapper to fire-and-forget.

    payment should contain: transId, time, amount, name, phone, accountNumber
    
    Only writes to predetermined accounts. Ignores requests for non-predetermined accounts.
    """
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEET_ID
    if not spreadsheet_id:
        logger.warning('No GOOGLE_SHEET_ID configured; skipping sheet write')
        return False

    # Validate account against predetermined list
    account_number = str(payment.get('accountNumber') or '')
    if not is_valid_account(account_number):
        logger.warning('Ignoring payment for non-predetermined account: %s (TransID: %s)', account_number, payment.get('transId'))
        return False

    try:
        logger.debug('Attempting to initialize Google Sheets service for write')
        service = _get_service(write=True)
        logger.debug('Google Sheets service initialized successfully')
    except Exception as e:
        logger.error('Failed to initialize Google Sheets service: %s', e, exc_info=True)
        return False

    safe_account = _sanitize_sheet_name(account_number)
    logger.debug('Sanitized account name: %s', safe_account)
    
    # Ensure sheet exists and check if it's new
    sheet_exists, is_new = _ensure_sheet_exists(service, spreadsheet_id, safe_account)
    if not sheet_exists:
        logger.error('Failed to ensure sheet %s exists', safe_account)
        return False

    # If sheet is new, write headers first
    if is_new:
        headers = ['Transaction ID', 'Time', 'Amount', 'Name']
        header_range = f"{safe_account}!A1:D1"
        header_body = {'values': [headers]}
        try:
            logger.debug('Writing headers to new sheet %s', safe_account)
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=header_range,
                valueInputOption='USER_ENTERED',
                body=header_body
            ).execute()
            logger.info('Successfully wrote headers to new sheet %s', safe_account)
        except Exception as e:
            logger.error('Failed to write headers to sheet %s: %s', safe_account, e)
            # Continue anyway; headers are just cosmetic

    row = [
        payment.get('transId') or '',
        payment.get('time') or '',
        payment.get('amount') or '',
        payment.get('name') or '',
    ]

    range_name = f"{safe_account}!A:D"
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


def check_transaction_exists(trans_id: str, spreadsheet_id: str = None) -> bool:
    """Check if a TransID already exists in any sheet in the spreadsheet.
    
    Returns True if found, False otherwise or on error.
    """
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEET_ID
    if not spreadsheet_id:
        logger.warning('No GOOGLE_SHEET_ID configured; cannot check for duplicate TransID')
        return False

    try:
        service = _get_service(write=False)
    except Exception as e:
        logger.error('Failed to initialize Sheets service for duplicate check: %s', e)
        return False

    try:
        # Get all sheets in the spreadsheet
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets.properties').execute()
        sheets = meta.get('sheets', [])
        
        # Search each sheet for the TransID
        for sheet in sheets:
            sheet_name = sheet.get('properties', {}).get('title')
            if not sheet_name:
                continue
            
            try:
                # Query the first column (A) for TransID matches
                result = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:A"
                ).execute()
                
                values = result.get('values', [])
                # Check if trans_id exists in column A (skip header if present)
                if trans_id in [str(v[0]) if v else '' for v in values]:
                    logger.info('Found duplicate TransID %s in sheet %s', trans_id, sheet_name)
                    return True
            except Exception as e:
                logger.debug('Error checking sheet %s for TransID %s: %s', sheet_name, trans_id, e)
                continue
        
        return False
    except Exception as e:
        logger.error('Error checking transaction existence: %s', e, exc_info=True)
        return False


def clear_cache():
    """Clear the accounts cache (useful for testing)."""
    _cache['accounts'] = None
    _cache['fetched_at'] = 0
