import os
import json
import logging
import time
from typing import Dict, Any
from datetime import datetime

import requests
from django.http import JsonResponse
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework import status

from .google_sheets import is_valid_account, write_payment_to_sheet, check_transaction_exists
from .serializers import DarajaC2BCallbackSerializer
from .config import GOOGLE_SHEET_ID, C2B_HTTP_TIMEOUT

logger = logging.getLogger(__name__)

# Config via centralized config module
SPREADSHEET_ID = GOOGLE_SHEET_ID

# Request timeouts
HTTP_TIMEOUT = C2B_HTTP_TIMEOUT


def _normalize_daraja_data(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Daraja callback data for sheet writing.
    
    - Formats phone numbers consistently (masks long hashes, formats Kenyan numbers)
    - Formats transaction time as readable date/time
    - Truncates or formats BillRefNumber if it's a long hash
    - Formats amount with currency
    - Cleans up name fields
    
    Returns normalized payment dict suitable for sheet writing.
    """
    trans_id = str(validated_data.get('TransID', ''))
    trans_time = validated_data.get('TransTime', '')
    trans_amount = float(validated_data.get('TransAmount', 0))
    bill_ref = str(validated_data.get('BillRefNumber', ''))
    phone = validated_data.get('MSISDN') or validated_data.get('Msisdn') or ''
    first_name = validated_data.get('FirstName', '').strip()
    middle_name = validated_data.get('MiddleName', '').strip()
    last_name = validated_data.get('LastName', '').strip()
    
    # Format phone number: mask long hashes, format regular numbers
    if phone:
        phone_str = str(phone)
        if len(phone_str) > 20:  # Likely a hash, truncate and indicate it's masked
            phone_formatted = f"{phone_str[:12]}... (masked)"
        else:
            # Kenyan numbers: format as +254XXXXXXXXX or 254XXXXXXXXX
            phone_str = phone_str.lstrip('0').lstrip('+')
            if not phone_str.startswith('254'):
                phone_formatted = f"+254{phone_str}" if len(phone_str) >= 9 else phone_str
            else:
                phone_formatted = f"+{phone_str}"
    else:
        phone_formatted = ''
    
    # Format transaction time: convert from YYYYMMDDHHmmss to readable format
    time_formatted = trans_time
    if trans_time and len(trans_time) >= 14:
        try:
            dt = datetime.strptime(trans_time[:14], '%Y%m%d%H%M%S')
            time_formatted = dt.strftime('%d/%m/%Y %H:%M:%S')
        except ValueError:
            logger.debug('Could not parse TransTime %s, using as-is', trans_time)
    
    # Format BillRefNumber: if it's a long hash, truncate it
    if bill_ref and len(bill_ref) > 20:
        bill_ref_display = f"{bill_ref[:12]}..."
    else:
        bill_ref_display = bill_ref
    
    # Combine name fields, clean up whitespace
    name = ' '.join(filter(None, [first_name, middle_name, last_name])) or 'N/A'
    
    # Format amount
    amount_formatted = f"KES {trans_amount:,.2f}"
    
    return {
        'transId': trans_id,
        'time': time_formatted,
        'amount': amount_formatted,
        'name': name,
        'phone': phone_formatted,
        'accountNumber': bill_ref,
        'rawAmount': trans_amount,  # Keep raw for calculations if needed
    }


def _daraja_response(code: int, desc: str):
    """Return Daraja-compliant JSON response."""
    return JsonResponse({
        "ResultCode": code,
        "ResultDesc": desc
    })


@csrf_exempt
@api_view(['POST'])
def daraja_c2b_callback(request):
    """Handle Daraja C2B payment callbacks.
    
    Workflow:
    1. Validate JSON payload against official Daraja fields
    2. Check for duplicate transactions
    3. Fetch valid accounts from Google Sheets (with fallback)
    4. Validate BillRefNumber against account list
    5. Forward to Apps Script for logging
    6. Return Daraja response code
    
    Endpoint: POST /api/daraja/c2b/
    """
    if request.method != 'POST':
        return _daraja_response(1, 'Rejected: Only POST allowed')

    # Parse and validate payload
    try:
        if isinstance(request.data, dict):
            payload = request.data
        else:
            payload = json.loads(request.body.decode('utf-8'))
    except Exception as e:
        logger.warning('Invalid JSON payload: %s', e)
        return _daraja_response(1, 'Rejected: Invalid JSON')

    serializer = DarajaC2BCallbackSerializer(data=payload)
    if not serializer.is_valid():
        errors = '; '.join([f"{k}: {v[0]}" for k, v in serializer.errors.items()])
        logger.error('PROD: Serializer validation failed. Payload keys: %s, Errors: %s', list(payload.keys()), errors)
        return _daraja_response(1, f'Rejected: {errors}')

    validated_data = serializer.validated_data
    bill_ref = str(validated_data.get('BillRefNumber'))
    trans_id = str(validated_data.get('TransID'))
    trans_amount = float(validated_data.get('TransAmount'))
    phone = validated_data.get('MSISDN') or validated_data.get('Msisdn') or ''
    
    logger.debug('PROD: Processing C2B. Payload keys: %s, TransID: %s, BillRefNumber: %s', list(payload.keys()), trans_id, bill_ref)

    # Duplicate check (query sheets for existing TransID)
    if check_transaction_exists(trans_id, spreadsheet_id=SPREADSHEET_ID):
        logger.info('Duplicate transaction in sheets: %s', trans_id)
        return _daraja_response(1, 'Rejected: Duplicate transaction')

    # Validate account against predetermined list
    if not is_valid_account(bill_ref):
        logger.info('Invalid BillRefNumber: %s (not in predetermined accounts)', bill_ref)
        return _daraja_response(1, 'Rejected: Invalid account')

    # Synchronous write to Google Sheets (was async, now blocking for reliability)
    try:
        # Normalize data for clean sheet output
        payment = _normalize_daraja_data(validated_data)
        success = write_payment_to_sheet(payment, spreadsheet_id=SPREADSHEET_ID)
        if not success:
            logger.error('PROD: Sheet write failed for TransID %s. Payment: %s', trans_id, payment)
            # Still return success to Daraja, but log the failure
    except Exception as e:
        logger.exception('PROD: Exception during sheet write for %s. Error: %s', trans_id, e)
        # Still return success to Daraja

    logger.info('Accepted transaction %s for account %s amount %.2f', trans_id, bill_ref, trans_amount)

    return _daraja_response(0, 'Accepted')


@api_view(['POST'])
def daraja_validation_endpoint(request):
    """Optional: Handle Daraja validation requests (before C2B).
    
    Daraja may send a validation request before attempting C2B.
    Respond immediately to unblock the request flow.
    """
    return _daraja_response(0, 'Validation successful')


@api_view(['POST'])
def daraja_test_sheet_write(request):
    """DEBUG ENDPOINT: Test synchronous sheet write.
    
    POST payload example:
    {
        "transId": "TEST123",
        "time": "20260105120000",
        "amount": "100.00",
        "name": "Test User",
        "phone": "254712345678",
        "accountNumber": "600000"
    }
    
    Only accepts predetermined accounts. Returns error for non-predetermined accounts.
    Returns diagnostics about the write attempt (even if failed).
    """
    try:
        if isinstance(request.data, dict):
            payload = request.data
        else:
            payload = json.loads(request.body.decode('utf-8'))
    except Exception as e:
        logger.warning('Invalid JSON in test write: %s', e)
        return Response(
            {'error': f'Invalid JSON: {e}', 'success': False},
            status=status.HTTP_400_BAD_REQUEST
        )

    # Validate account first
    account_number = str(payload.get('accountNumber', '') or payload.get('BillRefNumber', ''))
    if not is_valid_account(account_number):
        return Response(
            {
                'error': f'Invalid account: {account_number} is not in predetermined accounts',
                'success': False,
                'payload_received': payload,
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # Normalize the test payload (similar to production data)
    normalized_payment = _normalize_daraja_data(payload)
    
    # Call synchronous write (not async)
    from .google_sheets import write_payment_to_sheet
    success = write_payment_to_sheet(normalized_payment, spreadsheet_id=SPREADSHEET_ID)
    
    return Response(
        {
            'success': success,
            'message': 'Sheet write attempted (check server logs for details)',
            'payload_received': payload,
            'normalized_data': normalized_payment,
        },
        status=status.HTTP_200_OK
    )
