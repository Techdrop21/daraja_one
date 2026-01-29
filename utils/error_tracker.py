import logging
import traceback
from enum import Enum
from datetime import datetime

# Get the error logger
error_logger = logging.getLogger('daraja_errors')


class ErrorSource(Enum):
    """Enum for identifying the source of errors"""
    DARAJA_API = "Daraja API"
    OUR_ENDPOINT = "Our Endpoint"
    API_KEYS = "API Keys/Credentials"
    DATABASE = "Database"
    GOOGLE_SHEETS = "Google Sheets"
    SMS_GATEWAY = "SMS Gateway"
    VALIDATION = "Validation Error"
    UNKNOWN = "Unknown Error"


def log_transaction_error(error_source, error_message, exception=None, context=None):
    """
    Log transaction errors with source identification and context
    
    Args:
        error_source: ErrorSource enum value identifying where the error occurred
        error_message: Description of the error
        exception: The exception object (optional)
        context: Additional context dictionary (optional)
        
    Example:
        log_transaction_error(
            error_source=ErrorSource.DARAJA_API,
            error_message="Failed to initiate STK Push",
            exception=e,
            context={
                'phone': '0717950959',
                'amount': '100',
                'account': '001'
            }
        )
    """
    
    # Build the log message with timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Main error information
    log_parts = [
        f"ERROR SOURCE: {error_source.value}",
        f"MESSAGE: {error_message}"
    ]
    
    # Add context if provided
    if context:
        context_str = " | ".join([f"{k}: {v}" for k, v in context.items()])
        log_parts.append(f"CONTEXT: {context_str}")
    
    # Add exception details if provided
    if exception:
        log_parts.append(f"EXCEPTION: {str(exception)}")
        log_parts.append(f"TRACEBACK:\n{traceback.format_exc()}")
    
    log_message = " | ".join(log_parts)
    
    # Log the error
    error_logger.error(log_message)


def log_payment_error(phone, amount, account_id, error_source, error_message, exception=None):
    """
    Specialized method for logging payment/transaction failures
    
    Args:
        phone: Customer phone number
        amount: Transaction amount
        account_id: Account identifier
        error_source: ErrorSource enum value
        error_message: Description of what failed
        exception: The exception object (optional)
    """
    context = {
        'phone': phone,
        'amount': amount,
        'account': account_id
    }
    
    log_transaction_error(
        error_source=error_source,
        error_message=f"Payment Transaction Failed - {error_message}",
        exception=exception,
        context=context
    )


def log_sms_error(phone, message_content, error_source, error_message, exception=None):
    """
    Specialized method for logging SMS delivery failures
    
    Args:
        phone: Target phone number
        message_content: SMS content being sent
        error_source: ErrorSource enum value
        error_message: Description of what failed
        exception: The exception object (optional)
    """
    context = {
        'phone': phone,
        'message_preview': message_content[:50] + "..." if len(message_content) > 50 else message_content,
    }
    
    log_transaction_error(
        error_source=error_source,
        error_message=f"SMS Delivery Failed - {error_message}",
        exception=exception,
        context=context
    )


def log_sheets_error(error_message, operation, exception=None, context=None):
    """
    Specialized method for logging Google Sheets errors
    
    Args:
        error_message: Description of the error
        operation: The operation being performed (e.g., 'read', 'write', 'append')
        exception: The exception object (optional)
        context: Additional context dictionary (optional)
    """
    log_context = {'operation': operation}
    if context:
        log_context.update(context)
    
    log_transaction_error(
        error_source=ErrorSource.GOOGLE_SHEETS,
        error_message=error_message,
        exception=exception,
        context=log_context
    )
