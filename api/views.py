import os
import json
import logging
import time
from typing import Dict, Any

import requests
from django.http import JsonResponse
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework import status

from .google_sheets import is_valid_account, write_payment_async
from .serializers import DarajaC2BCallbackSerializer

logger = logging.getLogger(__name__)

# In-memory processed transaction IDs
_processed_transids = set()

# Config via env
SPREADSHEET_ID = os.environ.get('GOOGLE_SHEET_ID')

# Request timeouts
HTTP_TIMEOUT = float(os.environ.get('C2B_HTTP_TIMEOUT', '3.0'))


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
        logger.warning('Validation error: %s', errors)
        return _daraja_response(1, f'Rejected: {errors}')

    validated_data = serializer.validated_data
    bill_ref = str(validated_data.get('BillRefNumber'))
    trans_id = str(validated_data.get('TransID'))
    trans_amount = float(validated_data.get('TransAmount'))

    # Duplicate check
    if trans_id in _processed_transids:
        logger.info('Duplicate transaction: %s', trans_id)
        return _daraja_response(1, 'Rejected: Duplicate transaction')

    # Validate account against predetermined list
    if not is_valid_account(bill_ref):
        logger.info('Invalid BillRefNumber: %s (not in predetermined accounts)', bill_ref)
        return _daraja_response(1, 'Rejected: Invalid account')

    # Fire-and-forget write to Google Sheets; do not block Daraja response
    try:
        payment = {
            'transId': trans_id,
            'time': validated_data.get('TransTime') or '',
            'amount': trans_amount,
            'name': ' '.join(filter(None, [validated_data.get('FirstName'), validated_data.get('MiddleName'), validated_data.get('LastName')])),
            'phone': validated_data.get('MSISDN') or validated_data.get('MSISDN'),
            'accountNumber': bill_ref,
        }
        write_payment_async(payment, spreadsheet_id=SPREADSHEET_ID)
    except Exception:
        logger.exception('Failed to start background sheet write for %s', trans_id)

    # Mark as processed
    _processed_transids.add(trans_id)
    logger.info('Accepted transaction %s for account %s amount %.2f', trans_id, bill_ref, trans_amount)

    return _daraja_response(0, 'Accepted')


@api_view(['POST'])
def daraja_validation_endpoint(request):
    """Optional: Handle Daraja validation requests (before C2B).
    
    Daraja may send a validation request before attempting C2B.
    Respond immediately to unblock the request flow.
    """
    return _daraja_response(0, 'Validation successful')
