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

# Predetermined accounts with team information
# Format in .env: JSON with account numbers as keys
# Parsed as: [(AccountNo, TeamName, [phones]), ...]
PREDETERMINED_ACCOUNTS_JSON = os.environ.get('PREDETERMINED_ACCOUNTS_JSON', '')


def parse_predetermined_accounts() -> list:
    """Parse predetermined accounts from JSON environment variable.
    
    Format: 
    {
      "600000": {"team": "Sandbox Team", "phones": ["0723145610", "0723145611"]},
      "001": {"team": "Team One", "phones": ["0723145610", "0723145611"]}
    }
    
    Returns:
        List of tuples: [(AccountNo, TeamName, [PhoneNumbers]), ...]
    """
    if not PREDETERMINED_ACCOUNTS_JSON:
        return []
    
    accounts = []
    try:
        # Parse JSON
        accounts_data = json.loads(PREDETERMINED_ACCOUNTS_JSON)
        
        if not isinstance(accounts_data, dict):
            logger.error('PREDETERMINED_ACCOUNTS_JSON must be a JSON object (dict), got %s', type(accounts_data))
            return []
        
        for account_no, info in accounts_data.items():
            account_no = account_no.strip()
            
            # Validate structure
            if not isinstance(info, dict):
                logger.warning('Invalid account info for %s (expected dict): %s', account_no, info)
                continue
            
            team_name = info.get('team', '').strip()
            phones = info.get('phones', [])
            
            if not isinstance(phones, list):
                logger.warning('Invalid phones for account %s (expected list): %s', account_no, phones)
                continue
            
            phones = [p.strip() for p in phones if isinstance(p, str) and p.strip()]
            
            if account_no and team_name and phones:
                accounts.append((account_no, team_name, phones))
                logger.debug('Parsed account: %s -> %s (%d phones)', account_no, team_name, len(phones))
            else:
                logger.warning('Incomplete account data (skipping): %s -> %s with %d phones', account_no, team_name, len(phones))
        
        logger.info('Loaded %d predetermined accounts from JSON environment', len(accounts))
        return accounts
        
    except json.JSONDecodeError as e:
        logger.error('Failed to parse PREDETERMINED_ACCOUNTS_JSON: invalid JSON - %s', e)
        return []
    except Exception as e:
        logger.exception('Error parsing PREDETERMINED_ACCOUNTS_JSON: %s', e)
        return []

# ============================================================================
# Daraja C2B Configuration
# ============================================================================

# HTTP timeout for Daraja requests (in seconds)
C2B_HTTP_TIMEOUT = float(os.environ.get('C2B_HTTP_TIMEOUT', '3.0'))

# ============================================================================
# Onfon Media SMS Configuration
# ============================================================================

# Onfon Media API Key + Client ID (recommended authentication method)
ONFON_API_KEY = os.environ.get('ONFON_API_KEY', '')
ONFON_CLIENT_ID = os.environ.get('ONFON_CLIENT_ID', '')
ONFON_SENDER = os.environ.get('ONFON_SENDER', 'Daraja')

# Enable/disable SMS notifications
SMS_ENABLED = bool(ONFON_API_KEY and ONFON_CLIENT_ID)

if SMS_ENABLED:
    logger.info('SMS notifications enabled via Onfon Media (API Key authentication)')
else:
    logger.warning(
        'SMS notifications disabled. Set ONFON_API_KEY and ONFON_CLIENT_ID to enable.'
    )

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
