import os
import time
import logging
import re
from typing import List, Dict, Any, Optional, Tuple

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
from utils.error_tracker import log_sheets_error, log_transaction_error, ErrorSource

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
        error_msg = f'Google service account file not found: {keyfile!s}'
        log_sheets_error(
            error_message="Service account credentials file missing",
            operation="initialize_service",
            context={'keyfile': str(keyfile)}
        )
        raise RuntimeError(error_msg)

    scopes = ['https://www.googleapis.com/auth/spreadsheets'] if write else ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes)
    return build('sheets', 'v4', credentials=creds)


def _fetch_accounts_from_sheet() -> List[tuple]:
    """Fetch accounts from the 'Accounts' sheet in the Google Sheet.
    
    Expected sheet format:
    - Column A: Account Number
    - Column B: Team Phone (comma-separated or space-separated)
    - Column C: Team Name
    
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
            phones_str = str(row[1]).strip() if row[1] else ''
            team_name = str(row[2]).strip() if row[2] else ''
            
            if not account_number:
                logger.debug('Row %d has empty account number, skipping', row_idx)
                continue
            
            # Parse phone numbers (comma or space separated)
            phones = []
            if phones_str:
                # Try comma-separated first, then space-separated
                if ',' in phones_str:
                    phones_list = [p.strip() for p in phones_str.split(',') if p.strip()]
                else:
                    phones_list = [p.strip() for p in phones_str.split() if p.strip()]
                
                # Filter out invalid phone numbers (names, addresses, etc.)
                for phone in phones_list:
                    if _is_valid_phone_number(phone):
                        phones.append(phone)
                    else:
                        logger.warning('Invalid phone number in row %d account %s: "%s" (must have at least 7 digits)', 
                                      row_idx, account_number, phone)
            
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
    3. Prefer environment data when sheet accounts have no valid phones
    4. If sheet fetch fails, use environment configuration as fallback
    5. Return empty list if neither is available
    
    This ensures we always have a fallback to environment-configured accounts,
    especially when sheet data is incomplete or has invalid phones.
    """
    # Try to fetch from sheet first
    sheet_accounts = _fetch_accounts_from_sheet()
    env_accounts = parse_predetermined_accounts()
    
    if sheet_accounts:
        # Create a map of environment accounts for easy lookup
        env_account_map = {acc[0]: acc for acc in env_accounts}
        
        # Start with sheet accounts, but prefer environment when sheet has no phones
        merged_accounts = []
        sheet_account_numbers = {acc[0] for acc in sheet_accounts}
        
        for sheet_acc in sheet_accounts:
            acc_num, team_name, phones = sheet_acc
            
            # If sheet account has no phones but environment has one, prefer environment
            if not phones and acc_num in env_account_map:
                env_acc = env_account_map[acc_num]
                merged_accounts.append(env_acc)
                logger.debug('Preferred environment account %s (sheet has no valid phones)', acc_num)
            else:
                merged_accounts.append(sheet_acc)
        
        # Add any environment accounts not in the sheet
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


def _is_valid_phone_number(phone: str) -> bool:
    """Check if a string looks like a valid phone number.
    
    Valid phone numbers should have at least 7 digits (minimum for most countries).
    This filters out names, addresses, or other non-phone text.
    
    Args:
        phone: Phone number string to validate
        
    Returns:
        True if the string contains at least 7 digits, False otherwise
    """
    if not phone:
        return False
    
    # Extract digits only
    digits = ''.join(c for c in str(phone) if c.isdigit())
    
    # Valid phone numbers should have at least 7 digits
    return len(digits) >= 7


def _ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> tuple:
    """Check if sheet exists; if not, create it. Returns (exists, is_new, sheet_id)."""
    # Check existing sheets
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets.properties').execute()
    sheets = meta.get('sheets', [])
    
    for sheet in sheets:
        properties = sheet.get('properties', {})
        if properties.get('title') == sheet_name:
            return True, False, properties.get('sheetId')
    
    # Sheet doesn't exist, create it
    requests_body = {
        'requests': [
            {'addSheet': {'properties': {'title': sheet_name}}}
        ]
    }
    try:
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=requests_body
        ).execute()
        replies = response.get('replies', [])
        created_sheet_id = None
        if replies:
            created_sheet_id = replies[0].get('addSheet', {}).get('properties', {}).get('sheetId')
        return True, True, created_sheet_id  # Created successfully and is new
    except Exception:
        logger.exception('Failed to create sheet %s in spreadsheet %s', sheet_name, spreadsheet_id)
        return False, False, None


def write_payment_to_sheet(payment: Dict[str, Any], spreadsheet_id: str = None):
    """Write a payment row to the spreadsheet using UpdateBatch for optimal performance.
    
    Layout:
    - Row 1: Headers (Transaction ID, Time, Amount, Name)
    - Rows 2-3: Blank (for readability)
    - Rows 4+: Transactions (sorted descending, latest on top)
    
    Uses UpdateBatch to insert new rows at the top and sort automatically.
    
    payment should contain: transId, time, amount, name, accountNumber
    
    Only writes to predetermined accounts. Ignores requests for non-predetermined accounts.
    """
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEET_ID
    if not spreadsheet_id:
        logger.warning('No GOOGLE_SHEET_ID configured; skipping sheet write')
        log_sheets_error(
            error_message="No GOOGLE_SHEET_ID configured",
            operation="write_payment",
            context={'trans_id': payment.get('transId')}
        )
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
        log_sheets_error(
            error_message="Failed to initialize Google Sheets service for writing",
            operation="write_payment",
            exception=e,
            context={'trans_id': payment.get('transId'), 'account': account_number}
        )
        return False

    safe_account = _sanitize_sheet_name(account_number)
    logger.debug('Sanitized account name: %s', safe_account)
    
    # Ensure sheet exists and check if it's new
    sheet_exists, is_new, sheet_id = _ensure_sheet_exists(service, spreadsheet_id, safe_account)
    if not sheet_exists or sheet_id is None:
        logger.error('Failed to ensure sheet %s exists', safe_account)
        log_sheets_error(
            error_message="Failed to ensure sheet exists in spreadsheet",
            operation="ensure_sheet_exists",
            context={'sheet_name': safe_account, 'sheet_id': sheet_id, 'trans_id': payment.get('transId')}
        )
        return False

    headers = ['Transaction ID', 'Time', 'Amount', 'Name']
    batch_requests = []
    
    # If sheet is new, initialize with header and blank rows
    if is_new:
        # Create header row (row 1, index 0)
        header_values = [
            [{'userEnteredValue': {'stringValue': header}} for header in headers]
        ]
        
        batch_requests.append({
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': 4,
                },
                'rows': [{'values': header_values[0]}],
                'fields': 'userEnteredValue'
            }
        })
        
        # Create blank rows 2-3
        blank_row_values = ['' for _ in headers]
        blank_cells = [{'userEnteredValue': {'stringValue': ''}} for _ in headers]
        
        for blank_idx in range(1, 3):  # Rows 2-3 (indices 1-2)
            batch_requests.append({
                'updateCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': blank_idx,
                        'endRowIndex': blank_idx + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 4,
                    },
                    'rows': [{'values': blank_cells}],
                    'fields': 'userEnteredValue'
                }
            })
    
    # Prepare transaction row data
    trans_row = [
        payment.get('transId', ''),
        payment.get('time', ''),
        str(payment.get('amount', '')),
        payment.get('name', ''),
    ]
    
    # Fetch existing transactions and rewrite the block with the newest payment first.
    existing_rows = []
    if not is_new:
        try:
            existing_result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{safe_account}!A4:D'
            ).execute()
            existing_rows = existing_result.get('values', [])
        except Exception as e:
            logger.warning('Failed to fetch existing transactions for sheet %s: %s', safe_account, e)
            existing_rows = []

    all_rows = [trans_row]
    for row in existing_rows:
        normalized_row = [str(row[idx]) if idx < len(row) else '' for idx in range(len(headers))]
        all_rows.append(normalized_row)

    all_row_cells = [
        {'values': [{'userEnteredValue': {'stringValue': str(val)}} for val in row]}
        for row in all_rows
    ]

    # Write the full transaction block starting at row 4 (index 3).
    batch_requests.append({
        'updateCells': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 3,  # Row 4
                'endRowIndex': 3 + len(all_row_cells),
                'startColumnIndex': 0,
                'endColumnIndex': 4,
            },
            'rows': all_row_cells,
            'fields': 'userEnteredValue'
        }
    })
    
    # Execute batch update
    try:
        batch_body = {'requests': batch_requests}
        logger.debug('Executing batch update for sheet %s with %d requests', safe_account, len(batch_requests))
        
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=batch_body
        ).execute()
        
        logger.info('Successfully wrote payment %s to sheet %s (account: %s)', 
                   payment.get('transId'), spreadsheet_id, safe_account)
        return True
        
    except Exception as e:
        logger.error('Failed to batch update sheet %s: %s', safe_account, e, exc_info=True)
        log_sheets_error(
            error_message="Failed to batch update payment row to Google Sheet",
            operation="batch_update_rows",
            exception=e,
            context={'sheet_name': safe_account, 'trans_id': payment.get('transId'), 'amount': payment.get('amount')}
        )
        return False


def write_payment_async(payment: Dict[str, Any], spreadsheet_id: str = None):
    """Fire-and-forget write to Google Sheets using a background thread."""
    def _worker():
        try:
            write_payment_to_sheet(payment, spreadsheet_id=spreadsheet_id)
        except Exception as e:
            logger.exception('Background sheet write failed for payment %s', payment.get('transId'))
            log_sheets_error(
                error_message="Background sheet write task failed",
                operation="write_payment_async",
                exception=e,
                context={'trans_id': payment.get('transId')}
            )

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


def _fetch_current_balance_from_sheet(account_number: str, spreadsheet_id: str = None) -> float:
    """Fetch the current account balance from F2 of the account's sheet.
    
    Reads cell F2 which contains the calculated total account balance.
    
    Args:
        account_number: The account number (e.g., "001", "ACC 001")
        spreadsheet_id: The Google Sheet ID (uses GOOGLE_SHEET_ID if not provided)
        
    Returns:
        The current balance as a float, or 0 if not found
    """
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEET_ID
    if not spreadsheet_id:
        logger.warning('No GOOGLE_SHEET_ID configured; cannot fetch current balance')
        return 0.0
    
    safe_account = _sanitize_sheet_name(str(account_number))
    
    try:
        service = _get_service(write=False)
        
        # Fetch cell F2 which contains the current account balance
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{safe_account}!F2"  # F2 = Current Account Balance
        ).execute()
        
        values = result.get('values', [])
        
        # Extract balance from F1
        if values and len(values) > 0 and len(values[0]) > 0:
            balance_str = str(values[0][0]).strip()
        else:
            balance_str = '0'
        
        # Extract numeric value from balance string (remove 'Ksh', commas, spaces)
        balance_float = float(balance_str.replace('Ksh', '').replace(',', '').strip() or '0')
        logger.debug('Fetched current balance from F1 for account %s: %f', account_number, balance_float)
        return balance_float
        
    except Exception as e:
        logger.warning('Failed to fetch current balance for account %s: %s', account_number, e)
        return 0.0


def notify_team_via_sms(payment: Dict[str, Any]) -> bool:
    """Send SMS notification to team members when payment is received.
    
    Fetches current balance from F1 of the account's sheet, adds transaction amount,
    and sends SMS with full transaction details and new balance.
    
    Args:
        payment: Payment dict with keys: accountNumber, amount, name, transId, time, accountBalance
        
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
    
    logger.debug('Account %s phones: %s (team: %s)', account_number, phones, team_name)
    
    # Prefetch the current balance from F1 of the account's sheet
    current_balance = _fetch_current_balance_from_sheet(account_number, spreadsheet_id=GOOGLE_SHEET_ID)
    
    # Build SMS message with transaction details
    trans_id = payment.get('transId', '')
    amount = payment.get('amount', 0)
    payer_name = payment.get('name', 'Unknown')
    trans_time = payment.get('time', '')
    
    # Format amount as currency
    try:
        amount_float = float(amount)
    except (ValueError, TypeError):
        amount_float = 0.0
    formatted_amount = f"Ksh{amount_float:.2f}"
    
    # Calculate new account balance by adding transaction amount to current balance from F1
    try:
        current_balance_float = float(current_balance) if current_balance else 0.0
    except (ValueError, TypeError):
        current_balance_float = 0.0
    
    new_balance_float = current_balance_float + amount_float
    formatted_new_balance = f"Ksh {new_balance_float:.2f}"
    
    # Build full SMS message format
    # Format: UC3BS895V0 Confirmed, on 03/03/2026 04:25 PM Ksh270.00 received from Bancy , Account Number 003. The new account balance is Ksh 2026
    message = (
        f"{trans_id} Confirmed, on {trans_time} {formatted_amount} "
        f"received from {payer_name} , Account Number {account_number}. "
        f"The new account balance is {formatted_new_balance}"
    )
    
    # Send SMS to all team members
    all_sent = True
    for phone in phones:
        try:
            success = send_sms(phone, message)
            if not success:
                logger.error('Failed to send SMS to %s for account %s', phone, account_number)
                log_transaction_error(
                    error_source=ErrorSource.SMS_GATEWAY,
                    error_message="SMS delivery failed",
                    context={
                        'phone': phone,
                        'account': account_number,
                        'trans_id': trans_id,
                        'reason': 'send_sms returned false'
                    }
                )
                all_sent = False
        except Exception as e:
            logger.exception('Exception while sending SMS to %s: %s', phone, e)
            log_transaction_error(
                error_source=ErrorSource.SMS_GATEWAY,
                error_message="Exception during SMS delivery",
                exception=e,
                context={
                    'phone': phone,
                    'account': account_number,
                    'trans_id': trans_id
                }
            )
            all_sent = False
    
    if all_sent:
        logger.info('Successfully notified %d team members for account %s', len(phones), account_number)
    
    return all_sent


def _format_transaction_time(time_str: str) -> str:
    """Format transaction time from Daraja format to readable format.
    
    Input format: "20250112120000" (YYYYMMDDHHmmss) OR "03/03/2026 04:25 PM" (MM/DD/YYYY HH:MM AM/PM)
    Output format: "03/03/2026 04:25 PM"
    
    Args:
        time_str: Transaction time string in either format
        
    Returns:
        Formatted date time string in MM/DD/YYYY HH:MM AM/PM format, or original string if parsing fails
    """
    if not time_str:
        return ''
    
    try:
        from datetime import datetime
        
        # Try Daraja format first: "20250112120000" (YYYYMMDDHHmmss)
        if len(str(time_str)) == 14 and str(time_str).isdigit():
            dt = datetime.strptime(str(time_str), '%Y%m%d%H%M%S')
            # Format: MM/DD/YYYY HH:MM AM/PM
            return dt.strftime('%m/%d/%Y %I:%M %p')
        
        # If already in readable format, return as is
        return str(time_str)
    except Exception as e:
        logger.debug('Failed to format transaction time %s: %s', time_str, e)
        return str(time_str)


def parse_payment_message(message: str) -> Optional[Dict[str, Any]]:
    """Parse new message format for payment information.
    
    Expected format:
    UC3BS895V0 Confirmed, on 03/03/2026 04:25 PM Ksh270.00 received from Bancy , Account Number 003. The new account balance is Ksh 2026
    
    Extracts:
    - transId: UC3BS895V0
    - dateTime: 03/03/2026 04:25 PM
    - amount: 270.00 (numeric value without currency)
    - name: Bancy
    - accountNumber: 003
    - accountBalance: 2026 (numeric value without currency)
    
    Args:
        message: Message string to parse
        
    Returns:
        Dictionary with extracted fields or None if parsing fails
    """
    if not message:
        return None
    
    try:
        # Pattern to match the new format
        # UC3BS895V0 Confirmed, on MM/DD/YYYY HH:MM AM/PM Ksh###.## received from Name , Account Number ###. The new account balance is Ksh###
        pattern = r'(\w+)\s+Confirmed,\s+on\s+([\d/]+\s+[\d:]+\s+(?:AM|PM))\s+(Ksh[\d,]+\.?\d*)\s+received\s+from\s+([^,]+)\s*,\s*Account\s+Number\s+([^.]+)\.\s*The\s+new\s+account\s+balance\s+is\s+(Ksh[\d,]+\.?\d*)'
        
        match = re.search(pattern, message.strip(), re.IGNORECASE)
        if not match:
            logger.warning('Failed to parse payment message with new format: %s', message[:100])
            return None
        
        trans_id, date_time, amount_str, name, account_num, balance_str = match.groups()
        
        # Clean up amount: remove "Ksh" and commas
        amount_str = amount_str.replace('Ksh', '').replace(',', '').strip()
        amount = float(amount_str) if amount_str else 0.0
        
        # Clean up balance: remove "Ksh" and commas
        balance_str = balance_str.replace('Ksh', '').replace(',', '').strip()
        balance = balance_str if balance_str else '0'
        
        # Clean up account number and name
        account_num = account_num.strip()
        name = name.strip()
        
        return {
            'transId': trans_id,
            'time': date_time.strip(),  # Already in MM/DD/YYYY HH:MM AM/PM format
            'amount': amount,
            'name': name,
            'accountNumber': account_num,
            'accountBalance': balance,
        }
    except Exception as e:
        logger.error('Error parsing payment message: %s', e, exc_info=True)
        return None
