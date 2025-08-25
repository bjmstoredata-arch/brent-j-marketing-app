# db_utils.py
import sqlite3
import pandas as pd
import os
from security import sanitize_input

# Use this path for compatibility with Streamlit Cloud
DB_NAME = os.path.join(os.path.dirname(__file__), 'brent_j_marketing.db')

def create_tables():
    """Initializes the SQLite database and creates the necessary tables."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create clients table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            phone TEXT PRIMARY KEY,
            client_name TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create vins table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vins (
            vin_number TEXT PRIMARY KEY,
            client_phone TEXT,
            model TEXT,
            prod_yr TEXT,
            body TEXT,
            engine TEXT,
            code TEXT,
            transmission TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(client_phone) REFERENCES clients(phone) ON DELETE CASCADE
        )
    ''')

    # Create parts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY,
            vin_number TEXT,
            client_phone TEXT,
            part_name TEXT,
            part_number TEXT,
            quantity INTEGER,
            notes TEXT,
            date_added TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(vin_number) REFERENCES vins(vin_number) ON DELETE CASCADE,
            FOREIGN KEY(client_phone) REFERENCES clients(phone) ON DELETE CASCADE
        )
    ''')
    
    # Create part_suppliers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS part_suppliers (
            id INTEGER PRIMARY KEY,
            part_id INTEGER,
            supplier_name TEXT,
            buying_price REAL,
            selling_price REAL,
            delivery_time TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(part_id) REFERENCES parts(id) ON DELETE CASCADE
        )
    ''')
    
    # Add indexes for better performance and security
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_vins_client_phone ON vins(client_phone)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parts_client_phone ON parts(client_phone)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parts_vin_number ON parts(vin_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_part_suppliers_part_id ON part_suppliers(part_id)')
    
    conn.commit()
    conn.close()

def migrate_schema():
    """Remove deprecated columns from parts table if they exist"""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Check if columns exist before trying to remove them
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(parts)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'deposit' in columns:
                cursor.execute("ALTER TABLE parts DROP COLUMN deposit;")
            if 'balance' in columns:
                cursor.execute("ALTER TABLE parts DROP COLUMN balance;")
            conn.commit()
    except sqlite3.Error as e:
        print(f"Migration error: {e}")

def load_data():
    """Loads all data from the database into pandas DataFrames."""
    conn = sqlite3.connect(DB_NAME)

    df_clients = pd.read_sql_query("SELECT * FROM clients", conn)
    df_vins = pd.read_sql_query("SELECT * FROM vins", conn)
    df_parts = pd.read_sql_query("SELECT * FROM parts", conn)
    df_part_suppliers = pd.read_sql_query("SELECT * FROM part_suppliers", conn)

    conn.close()
    return df_clients, df_vins, df_parts, df_part_suppliers
