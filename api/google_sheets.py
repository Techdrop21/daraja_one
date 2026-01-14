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


def _get_service(write: bool = False):
    # Use centralized config
    keyfile = GOOGLE_SERVICE_ACCOUNT_FILE

    if not os.path.exists(keyfile):
        raise RuntimeError(f'Google service account file not found: {keyfile!s}')

    scopes = ['https://www.googleapis.com/auth/spreadsheets'] if write else ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes)
    return build('sheets', 'v4', credentials=creds)


def _fetch_accounts_from_sheet() -> List[tuple]:
    """Fetch accounts from the 'Accounts' sheet in the Google Sheet.
    
    Expected sheet format:
    - Column A: Account Number
    - Column B: Team Name
    - Column C: Team Phone (comma-separated or space-separated)
    
    Returns list of tuples: (AccountNumber, TeamName, [PhoneNumbers])
    Returns empty list on any error (for fallback).
    """
    if not GOOGLE_SHEET_ID:
        logger.debug('No GOOGLE_SHEET_ID configured; cannot fetch accounts from sheet')
        return []
    
    try:
        service = _get_service(write=False)
        logger.debug('Fetching accounts from Accounts sheet in spreadsheet %s', GOOGLE_SHEET_ID)
        
        # Fetch data from 'Accounts' sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range='Accounts!A:C'  # Columns: Account Number, Team Name, Team Phone
        ).execute()
        
        values = result.get('values', [])
        if not values:
            logger.warning('Accounts sheet is empty or does not exist')
            return []
        
        accounts = []
        # Skip header row (first row)
        for row_idx, row in enumerate(values[1:], start=2):
            if len(row) < 3:
                logger.debug('Row %d in Accounts sheet has fewer than 3 columns, skipping', row_idx)
                continue
            
            account_number = str(row[0]).strip() if row[0] else None
            team_name = str(row[1]).strip() if row[1] else ''
            phones_str = str(row[2]).strip() if row[2] else ''
            
            if not account_number:
                logger.debug('Row %d has empty account number, skipping', row_idx)
                continue
            
            # Parse phone numbers (comma or space separated)
            phones = []
            if phones_str:
                # Try comma-separated first, then space-separated
                if ',' in phones_str:
                    phones = [p.strip() for p in phones_str.split(',') if p.strip()]
                else:
                    phones = [p.strip() for p in phones_str.split() if p.strip()]
            
            accounts.append((account_number, team_name, phones))
            logger.debug('Loaded account: %s - %s with %d phone numbers', account_number, team_name, len(phones))
        
        logger.info('Successfully fetched %d accounts from Accounts sheet', len(accounts))
        return accounts
        
    except Exception as e:
        logger.warning('Failed to fetch accounts from sheet; will fall back to environment config: %s', e)
        return []


def get_predetermined_accounts() -> List[tuple]:
    """Return the predetermined account list from sheet with fallback to environment.

    Returns list of tuples: (AccountNumber, TeamName, [PhoneNumbers])
    
    Strategy:
    1. Fetch from 'Accounts' sheet in Google Sheets
    2. Merge with PREDETERMINED_ACCOUNTS_ENV to ensure complete coverage
    3. If sheet fetch fails, use environment configuration as fallback
    4. Return empty list if neither is available
    
    This ensures we always have a fallback to environment-configured accounts
    even if the sheet fetch partially succeeds (missing some account numbers).
    """
    # Try to fetch from sheet first
    sheet_accounts = _fetch_accounts_from_sheet()
    env_accounts = parse_predetermined_accounts()
    
    if sheet_accounts:
        # Use sheet accounts as primary source
        sheet_account_numbers = {acc[0] for acc in sheet_accounts}
        
        # Merge in any environment accounts that aren't in the sheet
        # This ensures we have a fallback for accounts defined only in .env
        merged_accounts = list(sheet_accounts)
        for env_acc in env_accounts:
            if env_acc[0] not in sheet_account_numbers:
                merged_accounts.append(env_acc)
                logger.debug('Merged environment account %s (not in sheet)', env_acc[0])
        
        logger.debug('Using %d accounts from Accounts sheet (merged with %d environment accounts)', 
                     len(sheet_accounts), len(env_accounts) - len(sheet_account_numbers))
        return merged_accounts
    
    # Fall back to environment configuration
    if env_accounts:
        logger.debug('Sheet fetch failed; using %d predetermined accounts from environment', len(env_accounts))
        return env_accounts
    
    # No accounts available
    logger.warning('No accounts configured; neither Accounts sheet nor PREDETERMINED_ACCOUNTS_ENV is available')
    return []


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
    # Convert amount to float if it's a string, then format
    try:
        amount_float = float(amount)
    except (ValueError, TypeError):
        amount_float = 0.0
    formatted_amount = f"Ksh{amount_float:.2f}"
    
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
