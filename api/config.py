"""Centralized environment configuration for the Daraja API.

All environment variables should be read here for consistency, validation, and documentation.
Provides sensible defaults where appropriate.
"""

import os
import json
import logging
import tempfile

logger = logging.getLogger(__name__)

# ============================================================================
# Google Sheets Configuration
# ============================================================================

# Detect if we have individual Google credential env vars
_has_google_env_vars = all([
    os.environ.get('GOOGLE_PROJECT_ID'),
    os.environ.get('GOOGLE_CLIENT_ID'),
    os.environ.get('GOOGLE_CLIENT_EMAIL'),
    os.environ.get('GOOGLE_PRIVATE_KEY_ID'),
    os.environ.get('GOOGLE_PRIVATE_KEY'),
])

# If env vars are provided, construct the service account JSON
if _has_google_env_vars:
    _creds_dict = {
        'type': 'service_account',
        'project_id': os.environ.get('GOOGLE_PROJECT_ID'),
        'private_key_id': os.environ.get('GOOGLE_PRIVATE_KEY_ID'),
        'private_key': os.environ.get('GOOGLE_PRIVATE_KEY').replace('\\n', '\n'),
        'client_email': os.environ.get('GOOGLE_CLIENT_EMAIL'),
        'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': f"https://www.googleapis.com/robot/v1/metadata/x509/{os.environ.get('GOOGLE_CLIENT_EMAIL')}",
        'universe_domain': 'googleapis.com',
    }
    # Write to a temporary file for google-auth to read
    _temp_creds = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
    json.dump(_creds_dict, _temp_creds)
    _temp_creds.close()
    GOOGLE_SERVICE_ACCOUNT_FILE = _temp_creds.name
    logger.debug('Using Google credentials from environment variables (temp file: %s)', GOOGLE_SERVICE_ACCOUNT_FILE)
else:
    # Fall back to looking for a file
    _default_keyfile = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 
        'daraja-sheet.json'
    )
    GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get(
        'GOOGLE_SERVICE_ACCOUNT_FILE',
        _default_keyfile
    )
    logger.debug('Using Google credentials from file: %s', GOOGLE_SERVICE_ACCOUNT_FILE)

# Google Sheets spreadsheet ID where payments are logged
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
if not GOOGLE_SHEET_ID:
    logger.warning(
        'GOOGLE_SHEET_ID not set in environment. '
        'Sheet writes will be skipped. Set this to enable logging to Google Sheets.'
    )

# How long to cache the list of valid accounts (in seconds)
ACCOUNTS_CACHE_TTL = int(os.environ.get('ACCOUNTS_CACHE_TTL', '120'))

# Comma-separated list of valid account numbers
# Falls back to hardcoded defaults if not set
PREDETERMINED_ACCOUNTS_ENV = os.environ.get('PREDETERMINED_ACCOUNTS')

# ============================================================================
# Daraja C2B Configuration
# ============================================================================

# HTTP timeout for Daraja requests (in seconds)
C2B_HTTP_TIMEOUT = float(os.environ.get('C2B_HTTP_TIMEOUT', '3.0'))

# ============================================================================
# Application Configuration
# ============================================================================

# Enable detailed debug logging for sheet operations
DEBUG_SHEETS = os.environ.get('DEBUG_SHEETS', 'false').lower() in ('true', '1', 'yes')

# ============================================================================
# Logging Configuration
# ============================================================================

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# ============================================================================
# Validation & Display
# ============================================================================

def log_configuration():
    """Log current configuration (safe mode - hides sensitive values)."""
    logger.info(
        'Configuration loaded: '
        'GOOGLE_SHEET_ID=%s, '
        'CACHE_TTL=%d, '
        'C2B_TIMEOUT=%.1f, '
        'DEBUG_SHEETS=%s, '
        'LOG_LEVEL=%s',
        '***' if GOOGLE_SHEET_ID else 'NOT SET',
        ACCOUNTS_CACHE_TTL,
        C2B_HTTP_TIMEOUT,
        DEBUG_SHEETS,
        LOG_LEVEL,
    )

def get_config_summary() -> dict:
    """Return a summary of configuration for debugging/logging."""
    return {
        'google_sheet_id': '***' if GOOGLE_SHEET_ID else 'NOT SET',
        'service_account_file': GOOGLE_SERVICE_ACCOUNT_FILE,
        'accounts_cache_ttl': ACCOUNTS_CACHE_TTL,
        'c2b_http_timeout': C2B_HTTP_TIMEOUT,
        'debug_sheets': DEBUG_SHEETS,
        'log_level': LOG_LEVEL,
    }
