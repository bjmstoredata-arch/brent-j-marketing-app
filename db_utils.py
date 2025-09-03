# db_utils.py
import sqlite3
import pandas as pd
import streamlit as st
import os
import io
import zipfile
from datetime import datetime
import json
import hashlib

# Use relative path for Streamlit Cloud
DB_NAME = 'brent_j_marketing.db'

@st.cache_resource
def get_db_connection():
    """
    Establishes and caches a single database connection.
    This ensures the connection is reused across reruns for performance.
    """
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 3000")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

def create_tables():
    """Initializes the SQLite database and creates the necessary tables."""
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'user',
                created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                last_login TEXT
            )
        ''')
        
        # Create clients table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                phone TEXT PRIMARY KEY,
                client_name TEXT,
                created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT,
                last_updated_by TEXT
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
                created_by TEXT,
                last_updated_by TEXT,
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
                created_by TEXT,
                last_updated_by TEXT,
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
                created_by TEXT,
                last_updated_by TEXT,
                FOREIGN KEY(part_id) REFERENCES parts(id) ON DELETE CASCADE
            )
        ''')
        
        # Create activity_log table for system administration
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                username TEXT,
                action TEXT,
                details TEXT,
                table_name TEXT,
                record_id TEXT,
                old_values TEXT,
                new_values TEXT
            )
        ''')
        
        # Add indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vins_client_phone ON vins(client_phone)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_parts_client_phone ON parts(client_phone)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_parts_vin_number ON parts(vin_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_part_suppliers_part_id ON part_suppliers(part_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_log_timestamp ON activity_log(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_log_username ON activity_log(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
        
        # Create default admin user
        admin_password_hash = hashlib.sha256("admin".encode()).hexdigest()
        cursor.execute('''
            INSERT OR IGNORE INTO users (username, password_hash, role) 
            VALUES (?, ?, ?)
        ''', ('admin', admin_password_hash, 'admin'))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")

def migrate_schema():
    """Remove deprecated columns from parts table if they exist"""
    conn = get_db_connection()
    if conn is None:
        return
    try:
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

@st.cache_data(ttl=300, show_spinner="Loading data...")
def load_data():
    """Loads all data from the database into pandas DataFrames."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    try:
        df_clients = pd.read_sql_query("SELECT * FROM clients", conn)
        df_vins = pd.read_sql_query("SELECT * FROM vins", conn)
        df_parts = pd.read_sql_query("SELECT * FROM parts", conn)
        df_part_suppliers = pd.read_sql_query("SELECT * FROM part_suppliers", conn)
        return df_clients, df_vins, df_parts, df_part_suppliers
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def export_filtered_data(filters=None, format_type='csv'):
    """Export filtered data based on provided filters"""
    conn = get_db_connection()
    if conn is None:
        return io.BytesIO(), 'application/zip'
    
    queries = {}
    if not filters or 'clients' in filters.get('include', ['clients', 'vins', 'parts', 'part_suppliers']):
        client_where = ""
        if filters and 'client_phone' in filters:
            client_where = f"WHERE phone = '{filters['client_phone']}'"
        queries['clients'] = f"SELECT * FROM clients {client_where}"
    
    if not filters or 'vins' in filters.get('include', ['clients', 'vins', 'parts', 'part_suppliers']):
        vin_where = ""
        if filters and 'client_phone' in filters:
            vin_where = f"WHERE client_phone = '{filters['client_phone']}'"
        if filters and 'vin_number' in filters:
            connector = "AND" if vin_where else "WHERE"
            vin_where += f" {connector} vin_number = '{filters['vin_number']}'"
        queries['vins'] = f"SELECT * FROM vins {vin_where}"
    
    if not filters or 'parts' in filters.get('include', ['clients', 'vins', 'parts', 'part_suppliers']):
        part_where = ""
        if filters and 'client_phone' in filters:
            part_where = f"WHERE client_phone = '{filters['client_phone']}'"
        if filters and 'vin_number' in filters:
            connector = "AND" if part_where else "WHERE"
            part_where += f" {connector} vin_number = '{filters['vin_number']}'"
        queries['parts'] = f"SELECT * FROM parts {part_where}"
    
    if not filters or 'part_suppliers' in filters.get('include', ['clients', 'vins', 'parts', 'part_suppliers']):
        supplier_where = ""
        queries['part_suppliers'] = f"SELECT * FROM part_suppliers {supplier_where}"
    
    data = {}
    try:
        for name, query in queries.items():
            data[name] = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error exporting filtered data: {e}")
        return io.BytesIO(), 'application/zip'
    
    if format_type == 'excel':
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for name, df in data.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=name[:31], index=False)
        output.seek(0)
        return output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    else:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            for name, df in data.items():
                if not df.empty:
                    csv_data = df.to_csv(index=False)
                    zip_file.writestr(f"{name}.csv", csv_data)
        zip_buffer.seek(0)
        return zip_buffer, 'application/zip'

def get_activity_logs(username=None, limit=100):
    """Get activity logs (admin only)"""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = "SELECT * FROM activity_log"
        params = []
        if username:
            query += " WHERE username = ?"
            params.append(username)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        print(f"Error getting activity logs: {e}")
        return pd.DataFrame()

def database_maintenance():
    """Perform database maintenance tasks"""
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        
        result = conn.execute("PRAGMA integrity_check").fetchone()
        return result[0] == "ok"
    except Exception as e:
        print(f"Database maintenance error: {e}")
        return False

def export_database_backup():
    """Export database to a backup file with timestamp"""
    backup_filename = f"backups/db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        # Get all data from all tables
        tables = ['clients', 'vins', 'parts', 'part_suppliers', 'users', 'activity_log']
        backup_data = {}
        
        for table in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                backup_data[table] = df.to_dict('records')
            except:
                # Table might not exist, skip it
                continue
        
        # Create backups directory if it doesn't exist
        os.makedirs('backups', exist_ok=True)
        
        # Save backup to file
        with open(backup_filename, 'w') as f:
            json.dump(backup_data, f, indent=2)
        
        return backup_filename
    except Exception as e:
        print(f"Backup error: {e}")
        return None
    finally:
        conn.close()

def import_database_backup(backup_file):
    """Import database from backup file"""
    try:
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)
        
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        
        # Restore data to tables
        for table, records in backup_data.items():
            if records:
                # Clear existing data
                cursor.execute(f"DELETE FROM {table}")
                
                # Insert backed up data
                for record in records:
                    columns = ', '.join(record.keys())
                    placeholders = ', '.join(['?' for _ in record])
                    values = list(record.values())
                    
                    cursor.execute(
                        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                        values
                    )
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Restore error: {e}")
        return False