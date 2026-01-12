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
    parse_predetermined_accounts,
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
    """Return the predetermined account list from environment or fallback.

    Returns list of tuples: (AccountNumber, TeamName, [PhoneNumbers])
    
    Priority:
    1. Parse from PREDETERMINED_ACCOUNTS env var (new format with team info)
    2. Fall back to FALLBACK_ACCOUNTS hardcoded list
    """
    # Try to parse from environment first
    env_accounts = parse_predetermined_accounts()
    if env_accounts:
        logger.debug('Using %d predetermined accounts from environment', len(env_accounts))
        return env_accounts
    
    # Fall back to hardcoded defaults
    logger.debug('No accounts in environment, using fallback accounts')
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


def notify_team_via_sms(payment: Dict[str, Any]) -> bool:
    """Send SMS notification to team members when payment is received.
    
    Args:
        payment: Payment dict with keys: accountNumber, amount, name, transId, time, phone
        
    Returns:
        True if all SMS were sent successfully, False if any failed
    """
    # Import here to avoid circular imports
    from .sms import send_sms
    from .config import SMS_ENABLED
    from datetime import datetime
    
    if not SMS_ENABLED:
        logger.debug('SMS notifications disabled; skipping team notification')
        return False
    
    account_number = str(payment.get('accountNumber') or '')
    if not account_number:
        logger.warning('Cannot notify team: no account number in payment')
        return False
    
    # Find the account and get team info
    accounts = get_predetermined_accounts()
    account_info = None
    for acc, team_name, phones in accounts:
        if acc == account_number:
            account_info = (team_name, phones)
            break
    
    if not account_info:
        logger.warning('Cannot notify team: account %s not found in predetermined accounts', account_number)
        return False
    
    team_name, phones = account_info
    
    # Build SMS message with confirmation format
    trans_id = payment.get('transId', '')
    amount = payment.get('amount', 0)
    payer_name = payment.get('name', 'Unknown')
    payer_phone = payment.get('phone', '')
    trans_time = payment.get('time', '')
    
    # Parse transaction time (format: "20250112120000" -> "9/1/26 at 11:45 AM")
    formatted_date = _format_transaction_time(trans_time)
    
    # Format amount as currency
    formatted_amount = f"Ksh{amount:.2f}"
    
    # Build confirmation message
    message = (
        f"{trans_id} Confirmed. on {formatted_date} {formatted_amount} "
        f"received from {payer_name} {payer_phone}. Account Number {account_number}"
    )
    
    # Send SMS to all team members
    all_sent = True
    for phone in phones:
        try:
            success = send_sms(phone, message)
            if not success:
                logger.error('Failed to send SMS to %s for account %s', phone, account_number)
                all_sent = False
        except Exception as e:
            logger.exception('Exception while sending SMS to %s: %s', phone, e)
            all_sent = False
    
    if all_sent:
        logger.info('Successfully notified %d team members for account %s', len(phones), account_number)
    
    return all_sent


def _format_transaction_time(time_str: str) -> str:
    """Format transaction time from Daraja format to readable format.
    
    Input format: "20250112120000" (YYYYMMDDHHmmss)
    Output format: "9/1/26 at 11:45 AM"
    
    Args:
        time_str: Transaction time string
        
    Returns:
        Formatted date time string, or original string if parsing fails
    """
    if not time_str or len(time_str) < 14:
        return time_str
    
    try:
        from datetime import datetime
        # Parse: "20250112120000" -> datetime object
        dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
        # Format: "9/1/26 at 11:45 AM"
        formatted = dt.strftime('%-m/%-d/%y at %I:%M %p').replace(' 0', ' ')
        return formatted
    except Exception as e:
        logger.debug('Failed to format transaction time %s: %s', time_str, e)
        return time_str
