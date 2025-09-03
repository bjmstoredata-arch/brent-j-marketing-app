# auth.py
from datetime import datetime
import streamlit as st
import hashlib
import sqlite3
import os
import json

# Use relative path for Streamlit Cloud
DB_NAME = 'brent_j_marketing.db'

def get_db_connection():
    """Get database connection"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

def authenticate_user(username, password):
    """Authenticate user credentials with hashing"""
    conn = get_db_connection()
    if conn is None:
        return False, None
    
    try:
        # Hash the provided password
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        cursor = conn.cursor()
        cursor.execute("SELECT password_hash, role FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        
        if result:
            # Compare hashed passwords
            if result[0] == password_hash:
                # Update last login
                cursor.execute("UPDATE users SET last_login = ? WHERE username = ?", 
                             (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), username))
                conn.commit()
                return True, result[1]
        return False, None
    except Exception as e:
        print(f"Authentication error: {e}")
        return False, None

def get_user_role(username):
    """Get user role"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error getting user role: {e}")
        return None

def log_activity(username, action, details, table_name=None, record_id=None, old_values=None, new_values=None):
    """Log user activities to database with detailed tracking"""
    conn = get_db_connection()
    if conn is None:
        return
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            '''INSERT INTO activity_log (timestamp, username, action, details, table_name, record_id, old_values, new_values) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (timestamp, username, action, details, table_name, record_id, 
             json.dumps(old_values) if old_values else None,
             json.dumps(new_values) if new_values else None)
        )
        conn.commit()
    except Exception as e:
        print(f"Error logging activity: {e}")

def init_session_state():
    defaults = {
        'authenticated': False,
        'username': None,
        'user_role': None,
        'login_attempted': False,
        'need_rerun': False,
        'last_activity': datetime.now(),
        'maintenance_run': None,
        'backup_created': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def login_form():
    """Display login form"""
    st.title("Brent J. Marketing - Login")
    
    with st.form("login_form", clear_on_submit=True):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
    
    if submitted:
        authenticated, role = authenticate_user(username, password)
        
        if authenticated:
            st.session_state.authenticated = True
            st.session_state.username = username
            st.session_state.user_role = role
            st.session_state.login_attempted = False
            log_activity(username, "login", "User logged in successfully")
            st.success("Login successful!")
            st.session_state.need_rerun = True
        else:
            st.session_state.login_attempted = True
            st.error("Invalid username or password")
            log_activity(username, "login_failed", "Failed login attempt")

def logout():
    """Logout user"""
    if st.session_state.authenticated:
        log_activity(st.session_state.username, "logout", "User logged out")
    
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.user_role = None
    st.session_state.login_attempted = False
    st.session_state.need_rerun = True

def require_login():
    """Require user to be logged in"""
    if not st.session_state.authenticated:
        login_form()
        st.stop()

def require_admin():
    """Require user to be admin"""
    require_login()
    if st.session_state.user_role != 'admin':
        st.error("Administrator access required")
        st.stop()