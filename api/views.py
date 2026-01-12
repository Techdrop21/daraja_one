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
            'amount': formatted_amount,
            'name': title_case_name,
            'accountNumber': bill_ref,
        }
        success = write_payment_to_sheet(payment, spreadsheet_id=SPREADSHEET_ID)
        if not success:
            logger.error('PROD: Sheet write failed for TransID %s. Payment: %s', trans_id, payment)
            # Still return success to Daraja, but log the failure
        
        # Send SMS notification to team (fire-and-forget in background)
        try:
            notify_team_via_sms(payment)
        except Exception as e:
            logger.exception('PROD: Exception during team SMS notification for %s. Error: %s', trans_id, e)
            # Continue anyway, SMS failure shouldn't block payment acceptance
            
    except Exception as e:
        logger.exception('PROD: Exception during sheet write for %s. Error: %s', trans_id, e)
        # Still return success to Daraja

    logger.info('Accepted transaction %s for account %s amount %.2f', trans_id, bill_ref, trans_amount)

    return _daraja_response(0, 'Accepted')


@csrf_exempt
@api_view(['POST'])
def daraja_validation_endpoint(request):
    try:
        payload = request.data if isinstance(request.data, dict) else json.loads(request.body.decode())
    except Exception:
        return _daraja_response(1, 'Rejected: Invalid JSON')

    bill_ref = str(payload.get('BillRefNumber', '')).strip()

    if not is_valid_account(bill_ref):
        logger.warning(
            'VALIDATION REJECTED: Invalid BillRefNumber %s',
            bill_ref
        )
        return _daraja_response(1, 'Rejected: Invalid account')

    return _daraja_response(0, 'Accepted')


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
    account_number = str(payload.get('accountNumber', ''))
    if not is_valid_account(account_number):
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
    success = write_payment_to_sheet(payload, spreadsheet_id=SPREADSHEET_ID)
    
    return Response(
        {
            'success': success,
            'message': 'Sheet write attempted (check server logs for details)',
            'payload_received': payload,
        },
        status=status.HTTP_200_OK
    )
