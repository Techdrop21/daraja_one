from rest_framework import serializers
import logging

logger = logging.getLogger(__name__)


class DarajaC2BCallbackSerializer(serializers.Serializer):
    """Validates Daraja C2B callback payload.
    
    Official fields from Safaricom M-Pesa C2B API:
    https://developer.safaricom.co.ke/docs#c2b-api
    
    Accepts both official callback format and Daraja simulation format.
    """
    TransactionType = serializers.CharField(required=False, allow_blank=True)
    TransID = serializers.CharField(required=True)
    TransTime = serializers.CharField(required=False, allow_blank=True)
    TransAmount = serializers.DecimalField(max_digits=10, decimal_places=2, required=True)
    BusinessShortCode = serializers.CharField(required=False)
    ShortCode = serializers.CharField(required=False)  # Simulation format
    BillRefNumber = serializers.CharField(required=True)
    InvoiceNumber = serializers.CharField(required=False, allow_blank=True)
    MSISDN = serializers.CharField(required=False)  # Official format
    Msisdn = serializers.CharField(required=False)  # Simulation format
    FirstName = serializers.CharField(required=False, allow_blank=True)
    MiddleName = serializers.CharField(required=False, allow_blank=True)
    LastName = serializers.CharField(required=False, allow_blank=True)
    OrgAccountBalance = serializers.DecimalField(max_digits=15, decimal_places=2, required=False, allow_null=True)
    CommandID = serializers.CharField(required=False, allow_blank=True)  # Simulation format

    def validate_TransAmount(self, value):
        if value <= 0:
            raise serializers.ValidationError("TransAmount must be a positive number.")
        return value

    def validate(self, data):
        """Normalize field names and cross-field validation."""
        # Normalize MSISDN (accept both Msisdn and MSISDN)
        if 'Msisdn' in data and 'MSISDN' not in data:
            data['MSISDN'] = data.pop('Msisdn')
        
        # Normalize BusinessShortCode (accept both ShortCode and BusinessShortCode)
        if 'ShortCode' in data and 'BusinessShortCode' not in data:
            data['BusinessShortCode'] = data.pop('ShortCode')
        
        logger.debug('Normalized validation data: %s', {k: v for k, v in data.items() if k not in ['OrgAccountBalance']})
        return data
