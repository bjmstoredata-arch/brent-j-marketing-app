# logic.py

import sqlite3
from datetime import datetime
from security import validate_phone, validate_vin, sanitize_input, validate_numeric

DB_NAME = 'brent_j_marketing.db'

def _execute_query(query, params=(), fetch=None):
    """A helper function to execute database queries with proper connection handling."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Set timeout to avoid database locks
            conn.execute("PRAGMA busy_timeout = 3000")
            cursor = conn.cursor()
            cursor.execute(query, params)
            if fetch == 'one':
                result = cursor.fetchone()
            elif fetch == 'all':
                result = cursor.fetchall()
            elif query.strip().upper().startswith('INSERT'):
                result = cursor.lastrowid
            else:
                result = None
                
            # Explicit commit for non-SELECT queries
            if not query.strip().upper().startswith('SELECT'):
                conn.commit()
                
            return result
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise  # Re-raise the exception to be handled by the caller

def add_new_client(phone, client_name):
    """Add a new client to the database with validation"""
    if not phone:
        raise ValueError("Phone number is required")
    
    if not validate_phone(phone):
        raise ValueError("Invalid phone number format")
    
    # Sanitize inputs
    phone = sanitize_input(phone)
    client_name = sanitize_input(client_name)
    
    # Check if client already exists
    existing_client = _execute_query(
        "SELECT phone FROM clients WHERE phone = ?", 
        (phone,), 
        fetch='one'
    )
    
    if existing_client:
        raise ValueError(f"Client with phone {phone} already exists")
    
    return _execute_query(
        "INSERT INTO clients (phone, client_name) VALUES (?, ?)", 
        (phone, client_name)
    )

def add_vin_to_client(client_phone, vin_no, model, prod_yr, body, engine, code, transmission):
    """Add a VIN to a client with validation"""
    if not client_phone:
        raise ValueError("Client phone is required")
    
    if not validate_phone(client_phone):
        raise ValueError("Invalid client phone format")
    
    # Clean and validate VIN
    if vin_no:
        clean_vin = ''.join(vin_no.split()).upper()
        if not validate_vin(clean_vin):
            raise ValueError("Invalid VIN format. Must be 7, 13, or 17 alphanumeric characters, or empty.")
    else:
        clean_vin = None  # Allow empty VIN
    
    # Sanitize inputs
    client_phone = sanitize_input(client_phone)
    clean_vin = sanitize_input(clean_vin) if clean_vin else None
    model = sanitize_input(model)
    prod_yr = sanitize_input(prod_yr)
    body = sanitize_input(body)
    engine = sanitize_input(engine)
    code = sanitize_input(code)
    transmission = sanitize_input(transmission)
    
    # Check if client exists
    client_exists = _execute_query(
        "SELECT phone FROM clients WHERE phone = ?", 
        (client_phone,), 
        fetch='one'
    )
    
    if not client_exists:
        raise ValueError(f"Client with phone {client_phone} does not exist")
    
    return _execute_query(
        "INSERT OR IGNORE INTO vins (vin_number, client_phone, model, prod_yr, body, engine, code, transmission) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (clean_vin, client_phone, model, prod_yr, body, engine, code, transmission)
    )

def add_supplier_to_part(part_id, supplier_name, buying_price, selling_price, delivery_time):
    """Add a supplier to a part with validation"""
    if not part_id:
        raise ValueError("Part ID is required")
    
    if not validate_numeric(part_id, min_val=1):
        raise ValueError("Invalid Part ID")
    
    if not supplier_name:
        raise ValueError("Supplier name is required")
    
    # Sanitize inputs
    supplier_name = sanitize_input(supplier_name)
    delivery_time = sanitize_input(delivery_time)
    
    # Validate numeric values
    if not validate_numeric(buying_price, min_val=0):
        raise ValueError("Invalid buying price")
    
    if not validate_numeric(selling_price, min_val=0):
        raise ValueError("Invalid selling price")
    
    return _execute_query(
        "INSERT INTO part_suppliers (part_id, supplier_name, buying_price, selling_price, delivery_time) VALUES (?, ?, ?, ?, ?)",
        (part_id, supplier_name, buying_price, selling_price, delivery_time)
    )

def add_part_to_vin(vin_number, client_phone, part_name, part_number, quantity, notes, suppliers):
    """Add a part to a VIN with transaction handling"""
    if not part_name and not part_number:
        raise ValueError("Part name or part number is required")
    
    date_added = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Using a single connection for the entire transaction
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO parts (vin_number, client_phone, part_name, part_number, quantity, notes, date_added) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (vin_number, client_phone, part_name, part_number, quantity, notes, date_added)
            )
            part_id = cursor.lastrowid
            
            for supplier in suppliers:
                cursor.execute(
                    "INSERT INTO part_suppliers (part_id, supplier_name, buying_price, selling_price, delivery_time) VALUES (?, ?, ?, ?, ?)",
                    (part_id, supplier['name'], supplier['buying_price'], supplier['selling_price'], supplier['delivery_time'])
                )
            conn.commit()
        return part_id
    except sqlite3.Error as e:
        print(f"Database error during part/supplier addition: {e}")
        raise

def safe_add_part_to_vin(vin_number, client_phone, part_data, suppliers):
    """
    Safely add a part to a VIN with comprehensive error handling
    
    Args:
        vin_number: VIN to add part to
        client_phone: Client phone number
        part_data: Dictionary with part details
        suppliers: List of supplier dictionaries
    
    Returns:
        part_id if successful, None otherwise
    """
    try:
        # Validate required fields
        if not part_data.get('name') and not part_data.get('number'):
            raise ValueError("Part name or number is required")
        
        if not validate_numeric(part_data.get('quantity', 0), min_val=1):
            raise ValueError("Quantity must be at least 1")
        
        # Add the part
        return add_part_to_vin(
            vin_number,
            client_phone,
            part_data['name'],
            part_data['number'],
            part_data['quantity'],
            part_data.get('notes', ''),
            suppliers
        )
    except Exception as e:
        print(f"Error adding part to VIN: {e}")
        raise

def add_part_without_vin(part_name, part_number, quantity, notes, client_phone, suppliers):
    """Add a part without a VIN with transaction handling"""
    if not part_name and not part_number:
        raise ValueError("Part name or part number is required")
    
    date_added = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO parts (part_name, part_number, quantity, notes, date_added, client_phone) VALUES (?, ?, ?, ?, ?, ?)",
                (part_name, part_number, quantity, notes, date_added, client_phone)
            )
            part_id = cursor.lastrowid
            
            for supplier in suppliers:
                cursor.execute(
                    "INSERT INTO part_suppliers (part_id, supplier_name, buying_price, selling_price, delivery_time) VALUES (?, ?, ?, ?, ?)",
                    (part_id, supplier['name'], supplier['buying_price'], supplier['selling_price'], supplier['delivery_time'])
                )
            conn.commit()
        return part_id
    except sqlite3.Error as e:
        print(f"Database error during part/supplier addition: {e}")
        raise

def delete_client(phone):
    """Delete a client and all associated data"""
    if not phone:
        raise ValueError("Phone number is required")
    
    return _execute_query("DELETE FROM clients WHERE phone = ?", (phone,))

def delete_vin(vin_number):
    """Delete a VIN and all associated parts"""
    if not vin_number:
        raise ValueError("VIN number is required")
    
    return _execute_query("DELETE FROM vins WHERE vin_number = ?", (vin_number,))

def delete_part(part_id):
    """Delete a part and all associated suppliers"""
    if not part_id:
        raise ValueError("Part ID is required")
    
    return _execute_query("DELETE FROM parts WHERE id = ?", (part_id,))

def update_client_and_vins(old_phone, new_phone, new_name):
    """Update client information and all associated references"""
    if not old_phone:
        raise ValueError("Old phone number is required")
    
    if not new_phone:
        raise ValueError("New phone number is required")
    
    # Using a single transaction for multiple updates
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE clients SET phone = ?, client_name = ? WHERE phone = ?", (new_phone, new_name, old_phone))
            cursor.execute("UPDATE vins SET client_phone = ? WHERE client_phone = ?", (new_phone, old_phone))
            cursor.execute("UPDATE parts SET client_phone = ? WHERE client_phone = ?", (new_phone, old_phone))
            conn.commit()
    except sqlite3.Error as e:
        print(f"Database error during client update: {e}")
        raise

def update_part(part_id, part_name, part_number, quantity, notes, suppliers_data):
    """Update part information and suppliers with transaction handling"""
    if not part_id:
        raise ValueError("Part ID is required")
    
    if not part_name and not part_number:
        raise ValueError("Part name or part number is required")
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE parts SET part_name = ?, part_number = ?, quantity = ?, notes = ? WHERE id = ?",
                (part_name, part_number, quantity, notes, part_id)
            )
            # Clear existing suppliers and add new ones in a single transaction
            cursor.execute("DELETE FROM part_suppliers WHERE part_id = ?", (part_id,))
            for supplier in suppliers_data:
                cursor.execute(
                    "INSERT INTO part_suppliers (part_id, supplier_name, buying_price, selling_price, delivery_time) VALUES (?, ?, ?, ?, ?)",
                    (part_id, supplier['name'], supplier['buying_price'], supplier['selling_price'], supplier['delivery_time'])
                )
            conn.commit()
    except sqlite3.Error as e:
        print(f"Database error during part update: {e}")
        raise