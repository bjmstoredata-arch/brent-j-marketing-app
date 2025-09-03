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
    """Lightweight sanitization for display purposes only.
    Do NOT use this for SQL – always use parameterized queries.
    """
    if text is None:
        return None
    return str(text).strip()


def validate_email(email):
    if not email:
        return False
    # simple, pragmatic check
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return re.match(pattern, str(email)) is not None

def normalize_vin(vin: str | None) -> str:
    """Uppercase and strip spaces from VINs before storing/comparing."""
    if not vin:
        return ""
    return re.sub(r"\s+", "", vin).upper()


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