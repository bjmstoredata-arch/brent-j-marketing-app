# security.py
import re

def validate_phone(phone):
    """Validate phone number format"""
    if not phone:
        return False
    pattern = r'^[\d\s\-\+\(\)]{7,15}$'
    return re.match(pattern, str(phone)) is not None

def validate_vin(vin):
    """Validate VIN format (basic check)"""
    if not vin or vin == "No VIN provided" or vin == "" or vin is None:
        return True  # Empty VIN is allowed
    
    # Clean the VIN - remove spaces and make uppercase
    clean_vin = ''.join(vin.split()).upper()
    
    # Check length
    if len(clean_vin) not in [0, 7, 13, 17]:
        return False
    
    # Check characters (only allowed characters)
    pattern = r'^[A-HJ-NPR-Z0-9]*$'  # Allow empty string too
    return re.match(pattern, clean_vin) is not None

def sanitize_input(text):
    """Basic sanitization to prevent SQL injection"""
    if not text:
        return text
    # Remove potentially dangerous SQL characters
    return re.sub(r'[;\-\-]', '', str(text))

def validate_email(email):
    """Validate email format"""
    if not email:
        return True
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_numeric(value, min_val=None, max_val=None):
    """Validate numeric values"""
    try:
        num = float(value)
        if min_val is not None and num < min_val:
            return False
        if max_val is not None and num > max_val:
            return False
        return True
    except (ValueError, TypeError):
        return False