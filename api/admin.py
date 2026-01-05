"""Admin/debug endpoint to inspect current configuration and diagnostics."""

import logging
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.views.decorators.csrf import csrf_exempt

from .config import get_config_summary, GOOGLE_SHEET_ID
from .google_sheets import get_predetermined_accounts, GOOGLE_SERVICE_ACCOUNT_FILE
import os

logger = logging.getLogger(__name__)


@csrf_exempt
@api_view(['GET'])
def config_status(request):
    """
    GET /api/config/status/
    
    Returns current configuration and diagnostics.
    Useful for debugging environment setup issues.
    """
    config = get_config_summary()
    
    # Check file existence
    service_account_exists = os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE)
    
    diagnostics = {
        'service_account_file': {
            'path': GOOGLE_SERVICE_ACCOUNT_FILE,
            'exists': service_account_exists,
        },
        'google_sheet_id_configured': bool(GOOGLE_SHEET_ID),
        'predetermined_accounts': get_predetermined_accounts(),
    }
    
    return Response(
        {
            'configuration': config,
            'diagnostics': diagnostics,
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
        },
        status=status.HTTP_200_OK
    )
