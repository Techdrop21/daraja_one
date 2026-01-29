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

from .google_sheets import is_valid_account, write_payment_to_sheet, check_transaction_exists, notify_team_via_sms
from .serializers import DarajaC2BCallbackSerializer
from .config import GOOGLE_SHEET_ID, C2B_HTTP_TIMEOUT
from utils.error_tracker import log_transaction_error, log_payment_error, log_sheets_error, ErrorSource

logger = logging.getLogger(__name__)

# Config via centralized config module
SPREADSHEET_ID = GOOGLE_SHEET_ID

# Request timeouts
HTTP_TIMEOUT = C2B_HTTP_TIMEOUT

def _format_transaction_time(trans_time: str) -> str:
    """Format transaction time to 'DD/MM/YYYY HH:MM PM/AM' format.
    
    Handles Daraja format: "20250110143025" (YYYYMMDDHHmmss)
    Returns: "10/01/2025 2:30 PM"
    """
    if not trans_time:
        return ''
    
    try:
        # Daraja format: YYYYMMDDHHmmss (20250110143025)
        if len(str(trans_time)) == 14:
            dt = datetime.strptime(str(trans_time), '%Y%m%d%H%M%S')
            # Format: DD/MM/YYYY HH:MM PM/AM
            return dt.strftime('%d/%m/%Y %I:%M %p')
        else:
            # If format is different, return as is
            return str(trans_time)
    except ValueError:
        logger.warning('Could not parse transaction time: %s', trans_time)
        return str(trans_time)

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
        log_transaction_error(
            error_source=ErrorSource.OUR_ENDPOINT,
            error_message="Invalid JSON payload received in C2B callback",
            exception=e,
            context={'payload': str(request.body)[:100]}
        )
        return _daraja_response(1, 'Rejected: Invalid JSON')

    serializer = DarajaC2BCallbackSerializer(data=payload)
    if not serializer.is_valid():
        errors = '; '.join([f"{k}: {v[0]}" for k, v in serializer.errors.items()])
        logger.error('PROD: Serializer validation failed. Payload keys: %s, Errors: %s', list(payload.keys()), errors)
        log_transaction_error(
            error_source=ErrorSource.VALIDATION,
            error_message="C2B callback payload validation failed",
            context={'errors': errors, 'payload_keys': str(list(payload.keys()))}
        )
        return _daraja_response(1, f'Rejected: {errors}')

    validated_data = serializer.validated_data
    bill_ref = str(validated_data.get('BillRefNumber'))
    trans_id = str(validated_data.get('TransID'))
    
    if not is_valid_account(bill_ref):
        logger.warning('BACKUP VALIDATION REJECTED: Invalid BillRefNumber %s in callback. TransID: %s', bill_ref, trans_id)
        log_transaction_error(
            error_source=ErrorSource.VALIDATION,
            error_message="Invalid account number received in C2B callback",
            context={'bill_ref': bill_ref, 'trans_id': trans_id}
        )
        return _daraja_response(1, 'Rejected: Invalid account number')
    trans_amount = float(validated_data.get('TransAmount'))
    trans_time = validated_data.get('TransTime') or ''
    
    logger.debug('PROD: Processing C2B. Payload keys: %s, TransID: %s, BillRefNumber: %s', list(payload.keys()), trans_id, bill_ref)

    # Synchronous write to Google Sheets (was async, now blocking for reliability)
    try:
        full_name = ' '.join(filter(None, [validated_data.get('FirstName'), validated_data.get('MiddleName'), validated_data.get('LastName')]))
        # Convert to title case (capitalize first letter of each word)
        title_case_name = full_name.title() if full_name else ''
        
        # Format time: Convert to "DD/MM/YYYY HH:MM PM/AM" format
        formatted_time = _format_transaction_time(trans_time)
        
        # Format amount: "KES XXX.XX"
        formatted_amount = f"KES {trans_amount:,.2f}"
        
        payment = {
            'transId': trans_id,
            'time': formatted_time,
            'amount': trans_amount,  # Keep as numeric value for SMS formatting
            'name': title_case_name,
            'accountNumber': bill_ref,
        }
        success = write_payment_to_sheet(payment, spreadsheet_id=SPREADSHEET_ID)
        if not success:
            logger.error('PROD: Sheet write failed for TransID %s. Payment: %s', trans_id, payment)
            log_sheets_error(
                error_message="Failed to write payment record to Google Sheets",
                operation="write_payment",
                context={
                    'trans_id': trans_id,
                    'account': bill_ref,
                    'amount': trans_amount
                }
            )
            # Still return success to Daraja, but log the failure
        
        # Send SMS notification to team (fire-and-forget in background)
        try:
            notify_team_via_sms(payment)
        except Exception as e:
            logger.exception('PROD: Exception during team SMS notification for %s. Error: %s', trans_id, e)
            log_transaction_error(
                error_source=ErrorSource.SMS_GATEWAY,
                error_message="Failed to send SMS notification to team",
                exception=e,
                context={'trans_id': trans_id, 'account': bill_ref}
            )
            # Continue anyway, SMS failure shouldn't block payment acceptance
            
    except Exception as e:
        logger.exception('PROD: Exception during sheet write for %s. Error: %s', trans_id, e)
        log_payment_error(
            phone=validated_data.get('MSISDN', 'Unknown'),
            amount=trans_amount,
            account_id=bill_ref,
            error_source=ErrorSource.GOOGLE_SHEETS,
            error_message="Exception during payment sheet write process",
            exception=e
        )
        # Still return success to Daraja

    logger.info('Accepted transaction %s for account %s amount %.2f', trans_id, bill_ref, trans_amount)

    return _daraja_response(0, 'Accepted')


@csrf_exempt
@api_view(['POST'])
def daraja_validation_endpoint(request):
    try:
        payload = request.data if isinstance(request.data, dict) else json.loads(request.body.decode('utf-8'))
        bill_ref = str(payload.get('BillRefNumber', '')).strip()
        
        logger.info('Validation request received. BillRefNumber: "%s" (raw: %s)', bill_ref, payload.get('BillRefNumber'))
        
        if not bill_ref:
            logger.warning('VALIDATION REJECTED: Blank/empty BillRefNumber')
            log_transaction_error(
                error_source=ErrorSource.VALIDATION,
                error_message="Validation endpoint received empty account number",
                context={'raw_value': str(payload.get('BillRefNumber'))}
            )
            return _daraja_response(1, 'Rejected: Account number required')
        
        if not is_valid_account(bill_ref):
            logger.warning('VALIDATION REJECTED: Invalid BillRefNumber "%s"', bill_ref)
            log_transaction_error(
                error_source=ErrorSource.VALIDATION,
                error_message="Validation endpoint rejected invalid account number",
                context={'bill_ref': bill_ref}
            )
            return _daraja_response(1, 'Rejected: Invalid account number')
        
        logger.info('Validation ACCEPTED for account: %s', bill_ref)
        return _daraja_response(0, 'Accepted')
    
    except Exception as e:
        logger.exception('Validation endpoint error: %s', e)
        log_transaction_error(
            error_source=ErrorSource.OUR_ENDPOINT,
            error_message="Exception in validation endpoint",
            exception=e
        )
        return _daraja_response(1, 'Rejected: System error')


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
        log_transaction_error(
            error_source=ErrorSource.OUR_ENDPOINT,
            error_message="Invalid JSON in test sheet write endpoint",
            exception=e
        )
        return Response(
            {'error': f'Invalid JSON: {e}', 'success': False},
            status=status.HTTP_400_BAD_REQUEST
        )

    # Validate account first
    account_number = str(payload.get('accountNumber', ''))
    if not is_valid_account(account_number):
        log_transaction_error(
            error_source=ErrorSource.VALIDATION,
            error_message="Test sheet write rejected - invalid account",
            context={'account_number': account_number}
        )
        return Response(
            {
                'error': f'Invalid account: {account_number} is not in predetermined accounts',
                'success': False,
                'payload_received': payload,
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # Call synchronous write (not async)
    from .google_sheets import write_payment_to_sheet
    try:
        success = write_payment_to_sheet(payload, spreadsheet_id=SPREADSHEET_ID)
        if not success:
            log_sheets_error(
                error_message="Test sheet write operation returned failure",
                operation="test_write",
                context={'account': account_number, 'trans_id': payload.get('transId')}
            )
    except Exception as e:
        logger.exception('Exception in test sheet write: %s', e)
        log_sheets_error(
            error_message="Exception during test sheet write",
            operation="test_write",
            exception=e,
            context={'account': account_number}
        )
    
    return Response(
        {
            'success': success,
            'message': 'Sheet write attempted (check server logs for details)',
            'payload_received': payload,
        },
        status=status.HTTP_200_OK
    )
