# app.py
import time
import sqlite3
import streamlit as st
import pandas as pd
from datetime import datetime
from db_utils import DB_NAME, load_data, create_tables, migrate_schema, export_filtered_data, database_maintenance, get_db_connection, get_activity_logs
from logic import (
    add_new_client, add_vin_to_client, add_part_to_vin,
    add_part_without_vin, delete_client, delete_vin,
    delete_part, update_client_and_vins, update_part,
    add_supplier_to_part, safe_add_part_to_vin, get_suppliers_for_part
)
from security import validate_phone, validate_vin, validate_numeric
from data_utils import safe_get_value, safe_get_first_row
from fpdf import FPDF
import random
import base64
import io
import zipfile
import json
import os
from auth import init_session_state, login_form, logout, require_login, require_admin

# Initialize session state
init_session_state()

# --- SESSION STATE INITIALIZATION ---
if 'view' not in st.session_state:
    st.session_state.view = 'main'
if 'show_client_form' not in st.session_state:
    st.session_state.show_client_form = False
if 'client_added' not in st.session_state:
    st.session_state.client_added = False
if 'vin_added' not in st.session_state:
    st.session_state.vin_added = False
if 'current_client_phone' not in st.session_state:
    st.session_state.current_client_phone = None
if 'current_client_name' not in st.session_state:
    st.session_state.current_client_name = None
if 'current_vin_no' not in st.session_state:
    st.session_state.current_vin_no = None
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'current_vin_no_view' not in st.session_state:
    st.session_state.current_vin_no_view = None
if 'add_part_mode' not in st.session_state:
    st.session_state.add_part_mode = False
if 'selected_vin_to_add_part' not in st.session_state:
    st.session_state.selected_vin_to_add_part = None
if 'part_count' not in st.session_state:
    st.session_state.part_count = 1
if 'supplier_count' not in st.session_state:
    st.session_state.supplier_count = 1
if 'supplier_count_edit' not in st.session_state:
    st.session_state.supplier_count_edit = 1
if 'current_part_id_to_add_supplier' not in st.session_state:
    st.session_state.current_part_id_to_add_supplier = None
if 'last_part_ids' not in st.session_state:
    st.session_state.last_part_ids = []
if 'generated_quote_msg' not in st.session_state:
    st.session_state.generated_quote_msg = ""
if 'quote_selected_vin' not in st.session_state:
    st.session_state.quote_selected_vin = None
if 'quote_selected_part_ids' not in st.session_state:
    st.session_state.quote_selected_part_ids = []
if 'quote_selected_phone' not in st.session_state:
    st.session_state.quote_selected_phone = None
if 'generated_text_quote' not in st.session_state:
    st.session_state.generated_text_quote = ""
if 'document_type' not in st.session_state:
    st.session_state.document_type = 'quote'
if 'generated_pdf_data' not in st.session_state:
    st.session_state.generated_pdf_data = None
if 'generated_pdf_filename' not in st.session_state:
    st.session_state.generated_pdf_filename = ""
if 'show_pdf_preview' not in st.session_state:
    st.session_state.show_pdf_preview = False
if 'part_conditions' not in st.session_state:
    st.session_state.part_conditions = {}
if 'clients_page' not in st.session_state:
    st.session_state.clients_page = 0
if 'parts_page' not in st.session_state:
    st.session_state.parts_page = 0
if 'action_history' not in st.session_state:
    st.session_state.action_history = []
if 'confirm_action' not in st.session_state:
    st.session_state.confirm_action = None
if 'export_filters' not in st.session_state:
    st.session_state.export_filters = {}
if 'current_filters' not in st.session_state:
    st.session_state.current_filters = {}
if 'selected_parts_suppliers' not in st.session_state:
    st.session_state.selected_parts_suppliers = {}
if 'current_part_management' not in st.session_state:
    st.session_state.current_part_management = {
        'current_part_index': 0,
        'parts_data': [],
        'saved_part_ids': []
    }
if 'maintenance_run' not in st.session_state:
    st.session_state.maintenance_run = None

# --- YOUR COMPANY INFO ---
COMPANY_INFO = {
    "name": "Brent J. Marketing",
    "distributor_line": "Distributors of European mechanical & body parts",
    "address_line1": "#46 Eastern Main Road, Silver Mill",
    "address_line2": "Trinidad and Tobago, San Juan",
    "specialties": [
        "3M reflective, aluminum shapes, sheets, safety equipment",
        "Traffic and road marking signage",
        "GLOUDS water pumps & parts"
    ],
    "phone1": "868-675-7294",
    "phone2": "868-713-2990",
    "phone3": "868-743-9004",
    "email": "brentjmarketingcompany@yahoo.com",
    "website": "bmwpartstt.com"
}

# --- Ensure tables are created when the app first runs ---
create_tables()
migrate_schema()

# Check if user is authenticated
if not st.session_state.authenticated:
    login_form()
    st.stop()

# --- SESSION TIMEOUT FUNCTIONALITY ---
# Initialize last activity time if not set
if 'last_activity' not in st.session_state:
    st.session_state.last_activity = datetime.now()

# Check for timeout (1 hour = 3600 seconds)
time_since_activity = (datetime.now() - st.session_state.last_activity).seconds

# Warn user 5 minutes before timeout
if time_since_activity > 3300:  # 55 minutes (5 minutes before timeout)
    time_remaining = 3600 - time_since_activity
    minutes_remaining = time_remaining // 60
    seconds_remaining = time_remaining % 60
    st.warning(f"Session will timeout in {minutes_remaining}m {seconds_remaining}s due to inactivity. Interact with the page to continue.")

# Full timeout after 1 hour
if time_since_activity > 3600:
    st.warning("Session timed out due to inactivity. Please log in again.")
    logout()
    st.rerun()

# Update last activity time on every interaction
st.session_state.last_activity = datetime.now()
# --- END SESSION TIMEOUT CODE ---

# Main application content
st.title("Brent J. Marketing, car parts database")
df_clients, df_vins, df_parts, df_part_suppliers = load_data()

# --- AUTO-BACKUP ON DEPLOYMENT ---
if 'backup_created' not in st.session_state:
    try:
        from db_utils import export_database_backup
        backup_file = export_database_backup()
        if backup_file:
            st.session_state.backup_created = True
            # Log silently - don't show to user on every load
    except:
        pass  

# --- DATABASE MAINTENANCE (Admin only, runs on Mondays) ---
if st.session_state.authenticated and st.session_state.user_role == 'admin':
    # Run maintenance weekly on Mondays
    if datetime.now().weekday() == 0:  # Monday (0 = Monday, 6 = Sunday)
        # Use a session state flag to only run once per day
        if 'maintenance_run' not in st.session_state or st.session_state.maintenance_run != datetime.now().date():
            if database_maintenance():
                st.success("✅ Weekly database maintenance completed!")
                from auth import log_activity
                log_activity(st.session_state.username, "maintenance", "Weekly database maintenance performed")
            else:
                st.error("❌ Database maintenance failed")
            st.session_state.maintenance_run = datetime.now().date()
# --- END MAINTENANCE CODE ---

# Add logout button to sidebar
st.sidebar.button("🚪 Logout", on_click=logout)
st.sidebar.write(f"Logged in as: {st.session_state.username} ({st.session_state.user_role})")

# Add admin menu if user is admin
if st.session_state.user_role == 'admin':
    if st.sidebar.button("📊 View Activity Logs"):
        st.session_state.view = 'activity_logs'
        st.session_state.need_rerun = True

# --- Activity Logs View (Admin Only) ---
if st.session_state.view == 'activity_logs':
    require_admin()
    st.header("Activity Logs")
    
    if st.button("⬅️ Back to Main"):
        st.session_state.view = 'main'
        st.session_state.need_rerun = True
    
    st.divider()
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        filter_username = st.text_input("Filter by username", "")
    with col2:
        log_limit = st.number_input("Number of logs", min_value=10, max_value=1000, value=100)
    
    # Load activity logs
    logs_df = get_activity_logs(filter_username if filter_username else None, log_limit)
    
    if not logs_df.empty:
        st.dataframe(logs_df, use_container_width=True)
        
        # Export option
        if st.button("Export Logs to CSV"):
            csv = logs_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"activity_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No activity logs found.")

def set_view(view_name):
    st.session_state.view = view_name
    st.session_state.need_rerun = True

def reset_part_management():
    """Reset part management state"""
    st.session_state.part_count = 1
    st.session_state.current_part_management = {
        'current_part_index': 0,
        'parts_data': [],
        'saved_part_ids': []
    }
    st.session_state.current_part_id_to_add_supplier = None
        
# --- PDF GENERATION FUNCTION ---
def generate_pdf(client_info, parts_data, total_quote_amount, manual_deposit, bill_to_info=None, ship_to_info=None, delivery_time=None, document_number=None, document_type='quote'):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=16)

    # --- COMPANY INFO HEADER ---
    pdf.set_font("Arial", style="B", size=14)
    pdf.cell(200, 8, txt=COMPANY_INFO["name"], ln=True, align="C")
    
    # Distributors line
    pdf.set_font("Arial", size=10)
    pdf.cell(200, 4, txt=COMPANY_INFO["distributor_line"], ln=True, align="C")

    # Address and Contact Info
    pdf.set_font("Arial", size=8)
    pdf.cell(200, 4, txt=COMPANY_INFO["address_line1"], ln=True, align="C")
    pdf.cell(200, 4, txt=COMPANY_INFO["address_line2"], ln=True, align="C")
    
    # Specialties line
    for specialty in COMPANY_INFO["specialties"]:
        pdf.cell(200, 4, txt=specialty, ln=True, align="C")
    
    pdf.cell(200, 4, txt=f"Phones: {COMPANY_INFO['phone1']} / {COMPANY_INFO['phone2']} / {COMPANY_INFO['phone3']}", ln=True, align="C")
    pdf.cell(200, 4, txt=f"Email: {COMPANY_INFO['email']}", ln=True, align="C")
    pdf.cell(200, 4, txt=f"Website: {COMPANY_INFO['website']}", ln=True, align="C")
    pdf.ln(5) # Add a line break for spacing

    # Quote Title, Number, and Date
    pdf.set_font("Arial", size=16)
    title_text = "QUOTATION" if document_type == 'quote' else "INVOICE"
    number_text = f"Quotation Number: {document_number}" if document_type == 'quote' else f"Invoice Number: {document_number}"
    
    pdf.cell(200, 10, txt=title_text, ln=True, align="C")
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=number_text, ln=True, align="R")
    pdf.cell(200, 10, txt=f"Date: {datetime.now().strftime('%Y-%m-%d')}", ln=True, align="R")
    pdf.ln(5)

    # --- BILL TO / SHIP TO SECTION ---
    current_y = pdf.get_y()
    
    # Bill To
    pdf.set_font("Arial", style="B", size=12)
    pdf.cell(100, 10, txt="Bill to:", ln=False, align="L")
    
    # Ship To
    pdf.set_x(110)
    pdf.cell(100, 10, txt="Ship to:", ln=True, align="L")
    
    pdf.set_font("Arial", size=12)
    pdf.set_y(current_y + 10)
    
    bill_to_name = bill_to_info['name'] if bill_to_info and bill_to_info['name'] else client_info['name']
    bill_to_address = bill_to_info['address'] if bill_to_info and bill_to_info['address'] else f"Phone: {client_info['phone']}"
    
    pdf.cell(100, 5, txt=f"{bill_to_name}", ln=False, align="L")
    
    ship_to_name = ship_to_info['name'] if ship_to_info and ship_to_info['name'] else ""
    ship_to_address = ship_to_info['address'] if ship_to_info and ship_to_info['address'] else ""
    
    pdf.set_x(110)
    pdf.cell(100, 5, txt=f"{ship_to_name}", ln=True, align="L")
    
    bill_to_address_lines = bill_to_address.split('\n')
    pdf.set_x(10)
    for line in bill_to_address_lines:
        pdf.cell(100, 5, txt=line, ln=True, align="L")
        
    ship_to_address_lines = ship_to_address.split('\n')
    pdf.set_xy(110, current_y + 15)
    for line in ship_to_address_lines:
        pdf.cell(100, 5, txt=line, ln=True, align="L")

    # Add VIN below the addresses, if applicable
    pdf.ln(5)
    if client_info['vin_number'] and client_info['vin_number'] != 'Show All Parts':
        pdf.cell(100, 5, txt=f"VIN: {client_info['vin_number']}", ln=True, align="L")
        pdf.ln(5)
    
    pdf.ln(5)

    # Parts Table Header
    pdf.set_font("Arial", style="B", size=12)
    pdf.cell(80, 10, txt="Part Name", border=1, align="C")
    pdf.cell(30, 10, txt="Quantity", border=1, align="C")
    pdf.cell(40, 10, txt="Unit Price ($)", border=1, align="C")
    pdf.cell(40, 10, txt="Total Price ($)", border=1, align="C", ln=True)

    # Parts Table Content
    pdf.set_font("Arial", size=12)
    for part in parts_data:
        total_price = part['quantity'] * part['price']
        
        pdf.cell(80, 10, txt=str(part['name']), border=1, align="L")
        pdf.cell(30, 10, txt=str(part['quantity']), border=1, align="C")
        pdf.cell(40, 10, txt=f"{part['price']:.2f}", border=1, align="R")
        pdf.cell(40, 10, txt=f"{total_price:.2f}", border=1, align="R", ln=True)

    # Total rows
    pdf.set_font("Arial", style="B", size=12)
    pdf.cell(150, 10, txt="TOTAL", border=1, align="R")
    pdf.cell(40, 10, txt=f"{total_quote_amount:.2f}", border=1, align="R", ln=True)

    if manual_deposit > 0:
        pdf.cell(150, 10, txt="DEPOSIT", border=1, align="R")
        pdf.cell(40, 10, txt=f"{manual_deposit:.2f}", border=1, align="R", ln=True)
        pdf.cell(150, 10, txt="BALANCE DUE", border=1, align="R")
        pdf.cell(40, 10, txt=f"{total_quote_amount - manual_deposit:.2f}", border=1, align="R", ln=True)

    # New Terms of Sale and Delivery
    pdf.ln(10)
    pdf.set_font("Arial", size=10)
    
    # Handle IN STOCK vs delivery time - FIXED
    if delivery_time == "IN STOCK":
        pdf.cell(200, 5, txt="* IN STOCK - Available for immediate pickup/shipment", ln=True)
    else:
        pdf.cell(200, 5, txt=f"* DELIVERY WITHIN {delivery_time} BUSINESS DAYS AFTER ORDER CONFIRMATION", ln=True)
    
    pdf.cell(200, 5, txt="* An 80% Deposit required upon Order Confirmation", ln=True)
    pdf.ln(5)
    pdf.set_font("Arial", style="B", size=10)
    pdf.cell(200, 5, txt="TERMS OF SALE:", ln=True)
    pdf.set_font("Arial", size=8)
    terms_text = (
        "No returns accepted after 7 days from invoice date. "
        "ALL Special Orders must be paid for in advance. "
        "A 20% Restocking Fee and Credit Card Fee applies to returned items. "
        "No return/refund on all special order items Electrical, electronic parts and fuel pumps, warranty is against the manufacture. "
        "Any charges incurred by this company in the recovery of any unpaid invoice balance on account or dishonoured cheque will be at the buyer's expense. "
        "The seller shall retain absolute title ownership and right to possession of the goods until full payment is received. "
        "A 2% finance charge for all account balances over 30 days. "
        "Shipping delays subject to airline, customs or natural disasters are not the responsibility of the seller."
    )
    pdf.multi_cell(0, 4, txt=terms_text)
    
    return pdf.output(dest='S')

def reset_view_and_state():
    st.session_state.view = 'main'
    st.session_state.show_client_form = False
    st.session_state.client_added = False
    st.session_state.vin_added = False
    st.session_state.current_client_phone = None
    st.session_state.current_client_name = None
    st.session_state.current_vin_no = None
    st.session_state.edit_mode = False
    st.session_state.current_vin_no_view = None
    st.session_state.add_part_mode = False
    st.session_state.selected_vin_to_add_part = None
    st.session_state.part_count = 1
    st.session_state.supplier_count = 1
    st.session_state.supplier_count_edit = 1
    st.session_state.current_part_id_to_add_supplier = None
    st.session_state.last_part_ids = []
    st.session_state.generated_quote_msg = ""
    st.session_state.quote_selected_vin = None
    st.session_state.quote_selected_part_ids = []
    st.session_state.quote_selected_phone = None
    st.session_state.generated_text_quote = ""
    st.session_state.document_type = 'quote'
    st.session_state.generated_pdf_data = None
    st.session_state.generated_pdf_filename = ""
    st.session_state.show_pdf_preview = False
    st.session_state.part_conditions = {}
    st.session_state.clients_page = 0
    st.session_state.parts_page = 0
    st.session_state.selected_items = {
        'clients': [],
        'vins': [],
        'parts': []
    }
    st.session_state.export_filters = {}
    st.session_state.current_filters = {}
    st.session_state.selected_parts_suppliers = {}
    st.session_state.need_rerun = True

def main_navigation():
    
    st.sidebar.title("Navigation")
    if st.sidebar.button("🏠 Main Dashboard"):
        set_view('main')
    
    if st.sidebar.button("👥 Clients"):
        st.session_state.view = 'client_list'
        st.session_state.need_rerun = True
    if st.sidebar.button("📦 Parts Inventory"):
        st.session_state.view = 'view_parts_inventory'
        st.session_state.need_rerun = True
    if st.sidebar.button("📄 Generate Quote"):
        st.session_state.view = 'generate_pdf_flow'
        st.session_state.document_type = 'quote'
        st.session_state.need_rerun = True
    if st.sidebar.button("🧾 Generate Invoice"):
        st.session_state.view = 'generate_pdf_flow'
        st.session_state.document_type = 'invoice'
        st.session_state.need_rerun = True
    if st.sidebar.button("📝 Text Quote"):
        st.session_state.view = 'generate_text_quote_flow'
        st.session_state.need_rerun = True
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"Logged in as: User")

def global_search():
    st.sidebar.markdown("---")
    st.sidebar.subheader("Global Search")
    search_term = st.sidebar.text_input("Search across all data")
    
    if search_term:
        # Search across clients, vins, and parts
        client_results = df_clients[df_clients['client_name'].str.contains(search_term, case=False) | 
                                   df_clients['phone'].str.contains(search_term, case=False)]
        
        vin_results = df_vins[df_vins['vin_number'].str.contains(search_term, case=False) | 
                             df_vins['model'].str.contains(search_term, case=False)]
        
        part_results = df_parts[df_parts['part_name'].str.contains(search_term, case=False) | 
                               df_parts['part_number'].str.contains(search_term, case=False)]
        
        if not client_results.empty or not vin_results.empty or not part_results.empty:
            st.sidebar.success(f"Found {len(client_results)} clients, {len(vin_results)} VINs, {len(part_results)} parts")
            
            if st.sidebar.button("View Search Results"):
                st.session_state.view = 'search_results'
                st.session_state.search_results = {
                    'clients': client_results,
                    'vins': vin_results,
                    'parts': part_results
                }
                st.session_state.need_rerun = True

def export_data():
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Export")
    
    # Enhanced export options
    export_type = st.sidebar.selectbox("Export Format", ["CSV (ZIP)", "Excel"])
    
    # Filter options for export
    with st.sidebar.expander("Export Filters"):
        st.write("Filter data to export:")
        include_tables = st.multiselect(
            "Include Tables",
            ["clients", "vins", "parts", "part_suppliers"],
            default=["clients", "vins", "parts", "part_suppliers"]
        )
        
        client_filter = st.text_input("Filter by Client Phone")
        vin_filter = st.text_input("Filter by VIN Number")
        
        filters = {}
        if client_filter:
            filters['client_phone'] = client_filter
        if vin_filter:
            filters['vin_number'] = vin_filter
        if include_tables:
            filters['include'] = include_tables
    
    if st.sidebar.button("Export Data"):
        with st.spinner("Preparing export..."):
            try:
                format_type = 'excel' if export_type == "Excel" else 'csv'
                data, mime_type = export_filtered_data(filters, format_type)
                
                file_ext = "xlsx" if export_type == "Excel" else "zip"
                file_name = f"brent_j_marketing_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_ext}"
                
                st.sidebar.download_button(
                    label=f"Download {export_type}",
                    data=data,
                    file_name=file_name,
                    mime=mime_type
                )
                from auth import log_activity
                log_activity("User", "export_data", f"Exported {export_type} with filters: {filters}")
            except Exception as e:
                st.sidebar.error(f"Export failed: {str(e)}")

def backup_database():
    import shutil
    import datetime
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Backup Management")
    
    # Manual backup
    if st.sidebar.button("Backup Database Now"):
        backup_name = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2('brent_j_marketing.db', backup_name)
        st.sidebar.success(f"Backup created: {backup_name}")
        from auth import log_activity
        log_activity("User", "backup", f"Created backup: {backup_name}")
    
    # List existing backups
    backups = [f for f in os.listdir('.') if f.startswith('backup_') and f.endswith('.db')]
    if backups:
        st.sidebar.write("**Existing Backups:**")
        for backup in sorted(backups, reverse=True)[:5]:  # Show only 5 most recent
            st.sidebar.write(f"• {backup}")

def confirm_action_interface():
    """Show confirmation dialog if needed"""
    if st.session_state.confirm_action:
        st.sidebar.warning(st.session_state.confirm_action['message'])
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("✅ Confirm"):
                st.session_state.confirm_action['action']()
                st.session_state.confirm_action = None
                st.session_state.need_rerun = True
        with col2:
            if st.button("❌ Cancel"):
                st.session_state.confirm_action = None
                st.session_state.need_rerun = True

def database_maintenance_interface():
    """Database maintenance utilities"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("Database Maintenance")
    
    if st.sidebar.button("Optimize Database"):
        if database_maintenance():
            st.sidebar.success("Database optimized!")
            from auth import log_activity
            log_activity("User", "maintenance", "Database optimization")
        else:
            st.sidebar.error("Database optimization failed")
    
    if st.sidebar.button("Check Database Integrity"):
        with sqlite3.connect(DB_NAME) as conn:
            result = conn.execute("PRAGMA integrity_check").fetchone()
        if result[0] == "ok":
            st.sidebar.success("Database integrity: OK")
        else:
            st.sidebar.error(f"Database issues: {result[0]}")
        from auth import log_activity
        log_activity("User", "maintenance", "Database integrity check")

def get_supplier_info(part_id, supplier_idx):
    """Get supplier information by index"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    suppliers = pd.read_sql_query(
        "SELECT * FROM part_suppliers WHERE part_id = ? ORDER BY supplier_name",
        conn, params=[part_id]
    )
    
    if not suppliers.empty and supplier_idx < len(suppliers):
        return suppliers.iloc[supplier_idx]
    return None

# --- UI LOGIC ---

df_clients, df_vins, df_parts, df_part_suppliers = load_data()

# Add navigation sidebar
main_navigation()
global_search()
export_data()
backup_database()
confirm_action_interface()
database_maintenance_interface()

# --- SEARCH AND MAIN SCREEN ---
if st.session_state.view == 'main':
    st.header("Quick Actions")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("➕ Add Client", help="Add a new client"):
            st.session_state.view = 'add_client'
            st.session_state.need_rerun = True
    with col2:
        if st.button("🔍 Find Client", help="Search for a client"):
            st.session_state.view = 'client_list'
            st.session_state.need_rerun = True
    
    st.divider()
    
    st.header("Search for Client")
    search_term = st.text_input("Search by exact Phone Number", key="search_clients")

    if search_term:
        with st.spinner("Searching for client..."):
            found_client = df_clients[df_clients['phone'].astype(str) == search_term]
            if not found_client.empty:
                st.session_state.view = 'client_details'
                st.session_state.current_client_phone = found_client['phone'].iloc[0]
                st.session_state.current_client_name = found_client['client_name'].iloc[0]
                st.session_state.need_rerun = True
            else:
                st.warning("No client found with that exact phone number.")
                if st.button("Register New Client"):
                    st.session_state.view = 'add_client'
                    st.session_state.show_client_form = True
                    st.session_state.need_rerun = True
    
    st.divider()

    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    with col1:
        if st.button("Add New Client"):
            st.session_state.show_client_form = True
            st.session_state.view = 'add_client'
            st.session_state.need_rerun = True
    with col2:
        if st.button("View All Clients"):
            st.session_state.view = 'client_list'
            st.session_state.need_rerun = True
    with col3:
        if st.button("View Parts"):
            st.session_state.view = 'view_parts_inventory'
            st.session_state.need_rerun = True
    with col4:
        if st.button("Generate Quote (PDF)"):
            st.session_state.view = 'generate_pdf_flow'
            st.session_state.document_type = 'quote'
            st.session_state.need_rerun = True
    with col5:
        if st.button("Generate Invoice (PDF)"):
            st.session_state.view = 'generate_pdf_flow'
            st.session_state.document_type = 'invoice'
            st.session_state.need_rerun = True

# --- Generate Quote/Invoice Flow ---
elif st.session_state.view == 'generate_pdf_flow':
    doc_type = st.session_state.document_type
    st.header(f"Generate {doc_type.capitalize()} (PDF)")
    if st.button("⬅️ Back to Main"):
        reset_view_and_state()
    
    st.divider()

    df_clients, df_vins, df_parts, df_part_suppliers = load_data()

    # Step 1: Select a Client
    client_options = [''] + sorted(df_clients['phone'].dropna().unique().tolist())
    st.session_state.quote_selected_phone = st.selectbox("Select Client Phone Number:", options=client_options, key="pdf_phone_selector")

    if st.session_state.quote_selected_phone:
        selected_client_name = df_clients[df_clients['phone'].astype(str) == st.session_state.quote_selected_phone]['client_name'].iloc[0]
        st.subheader(f"Parts for Client: `{selected_client_name}`")

        # Step 2: Allow VIN selection for that client
        vins_for_client = df_vins[df_vins['client_phone'].astype(str) == st.session_state.quote_selected_phone]['vin_number'].tolist()
        vin_options = ['Show All Parts'] + sorted(vins_for_client)
        st.session_state.quote_selected_vin = st.selectbox("Filter by VIN (optional):", options=vin_options, key="pdf_vin_selector")

        # Step 3: Get parts based on selections
        if st.session_state.quote_selected_vin == 'Show All Parts':
            parts_to_display = df_parts[df_parts['client_phone'].astype(str) == st.session_state.quote_selected_phone]
        else:
            parts_to_display = df_parts[
                (df_parts['client_phone'].astype(str) == st.session_state.quote_selected_phone) &
                (df_parts['vin_number'] == st.session_state.quote_selected_vin)
            ]
        
        if not parts_to_display.empty:
            
            selected_parts_data = {}  # Store part_id -> selected_supplier_index
            all_delivery_times = set()  # Collect all unique delivery times
            
            st.markdown("---")
            st.markdown("### Select Parts and Suppliers to Include in Document:")
            
            for _, part_row in parts_to_display.iterrows():
                # Get all suppliers for this part
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                
                if not part_suppliers.empty:
                    # Create expander for each part
                    with st.expander(f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']}"):
                        # Display part info
                        st.write(f"**Part:** {part_row['part_name']}")
                        st.write(f"**Number:** {part_row['part_number']}")
                        st.write(f"**Quantity:** {part_row['quantity']}")
                        if pd.notna(part_row['vin_number']):
                            st.write(f"**VIN:** {part_row['vin_number']}")
                        
                        st.markdown("---")
                        st.write("**Available Suppliers:**")
                        
                        # Create radio buttons for supplier selection
                        supplier_options = []
                        supplier_details = []  # Store supplier details for reference
                        for idx, supplier in part_suppliers.iterrows():
                            option_text = f"{supplier['supplier_name']} - ${supplier['selling_price']:.2f} - {supplier['delivery_time'] or 'No delivery time'}"
                            supplier_options.append(option_text)
                            supplier_details.append({
                                'idx': idx,
                                'name': supplier['supplier_name'],
                                'price': supplier['selling_price'],
                                'delivery_time': supplier['delivery_time'] or 'No delivery time'
                            })
                            
                            # Collect delivery time for global selection
                            if supplier['delivery_time'] and supplier['delivery_time'].strip():
                                all_delivery_times.add(supplier['delivery_time'])
                        
                        # Default to first supplier
                        default_idx = 0
                        supplier_choice = st.radio(
                            f"Select supplier for {part_row['part_name']}",
                            options=supplier_options,
                            index=default_idx,
                            key=f"pdf_supplier_choice_{part_row['id']}"
                        )
                        
                        # Find the selected supplier details
                        selected_index = supplier_options.index(supplier_choice)
                        selected_supplier = supplier_details[selected_index]
                        
                        # Checkbox to include this part
                        include_part = st.checkbox(
                            "Include this part in document",
                            value=True,
                            key=f"pdf_include_part_{part_row['id']}"
                        )
                        
                        if include_part:
                            selected_parts_data[part_row['id']] = {
                                'supplier_idx': selected_supplier['idx'],
                                'price': selected_supplier['price'],
                                'delivery_time': selected_supplier['delivery_time'],
                                'supplier_name': selected_supplier['name']
                            }
                else:
                    # For parts without suppliers
                    part_label = f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']} - No suppliers available"
                    if pd.notna(part_row['vin_number']):
                        part_label += f" (VIN: {part_row['vin_number']})"
                    
                    if st.checkbox(part_label, key=f"pdf_checkbox_{part_row['id']}"):
                        selected_parts_data[part_row['id']] = {
                            'supplier_idx': None,
                            'price': 0.0,
                            'delivery_time': None,
                            'supplier_name': "No supplier"
                        }

            st.markdown("---")
            
            # Delivery time selection (only show if we have delivery times)
            delivery_options_set = set()
            for part_id, supplier_data in selected_parts_data.items():
                if supplier_data['delivery_time'] and supplier_data['delivery_time'].strip():
                    delivery_options_set.add(supplier_data['delivery_time'])

            # Add "IN STOCK" as an option
            delivery_options_set.add("IN STOCK")
            delivery_options = sorted(list(delivery_options_set))

            # Default to "IN STOCK" if available, otherwise first option
            default_delivery_index = delivery_options.index("IN STOCK") if "IN STOCK" in delivery_options else 0

            if len(delivery_options) > 1:
                st.subheader("Select Delivery Time")
                selected_delivery_time = st.selectbox(
                    "Choose the delivery time to display in the document:",
                    options=delivery_options,
                    index=default_delivery_index,
                    key="pdf_delivery_time_selector"
                )
            else:
                selected_delivery_time = delivery_options[0] if delivery_options else "IN STOCK"
            
            # --- CUSTOM BILL TO / SHIP TO INPUTS ---
            st.subheader("Personalize Document Details")
            
            # Use columns for a cleaner layout
            col_bill, col_ship, col_other = st.columns([1, 1, 1])
            
            with col_bill:
                customize_bill = st.checkbox("Customize 'Bill To'", key="customize_bill_to")
                bill_to_name = selected_client_name
                bill_to_address = f"Phone: {st.session_state.quote_selected_phone}"
                
                if customize_bill:
                    bill_to_name = st.text_input("Bill To Name", value=selected_client_name, key="bill_to_name")
                    bill_to_address = st.text_area("Bill To Address", key="bill_to_address")
            
            with col_ship:
                customize_ship = st.checkbox("Customize 'Ship To'", key="customize_ship_to")
                ship_to_name = ""
                ship_to_address = ""
                
                if customize_ship:
                    ship_to_name = st.text_input("Ship To Name", key="ship_to_name")
                    ship_to_address = st.text_area("Ship To Address", key="ship_to_address")
            
            with col_other:
                # Manual deposit input
                manual_deposit = st.number_input("Manual Deposit ($)", min_value=0.0, value=0.0, format="%.2f", key="manual_deposit")

            # --- END OF CUSTOM INPUTS ---

            # Step 4: Generate the document
            if st.button(f"Generate {doc_type.capitalize()} (PDF)"):
                if not selected_parts_data:
                    st.warning(f"Please select at least one part to generate a {doc_type}.")
                else:
                    # Generate the document number
                    if doc_type == 'quote':
                        document_number = f"BJM-{random.randint(1000, 9999)}-Q"
                    else:
                        document_number = f"BJM-{random.randint(1000, 9999)}-I"

                    # Gather data for the PDF
                    client_info = {
                        'name': selected_client_name,
                        'phone': st.session_state.quote_selected_phone,
                        'vin_number': st.session_state.quote_selected_vin,
                    }
                    parts_data = []
                    total_quote_amount = 0
                    
                    for part_id, supplier_data in selected_parts_data.items():
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        
                        # Use the stored price instead of querying again
                        selling_price = supplier_data['price']
                        
                        total_quote_amount += (part_row['quantity'] * selling_price)
                        
                        parts_data.append({
                            'name': part_row['part_name'],  # Just the part name, no supplier info
                            'quantity': part_row['quantity'],
                            'price': selling_price,
                        })
                    
                    # Package the custom info
                    bill_to_info = {"name": bill_to_name, "address": bill_to_address} if customize_bill else None
                    ship_to_info = {"name": ship_to_name, "address": ship_to_address} if customize_ship else None

                    # Generate PDF
                    with st.spinner(f"Generating {doc_type}..."):
                        pdf_bytes = bytes(generate_pdf(client_info, parts_data, total_quote_amount, manual_deposit, bill_to_info, ship_to_info, selected_delivery_time, document_number, doc_type))
                    
                    # Store PDF data in session state for preview and download
                    st.session_state.generated_pdf_data = pdf_bytes
                    st.session_state.generated_pdf_filename = f"{st.session_state.quote_selected_phone}_{document_number}.pdf"
                    st.session_state.show_pdf_preview = True
            
            # Show PDF preview if available
            if st.session_state.get('show_pdf_preview', False) and st.session_state.get('generated_pdf_data'):
                st.markdown("---")
                st.subheader("PDF Preview")
                
                # Display download button
                st.download_button(
                    label=f"Download {doc_type.capitalize()}",
                    data=st.session_state.generated_pdf_data,
                    file_name=st.session_state.generated_pdf_filename,
                    mime="application/pdf"
                )
                
                # Display PDF preview using HTML embed
                base64_pdf = base64.b64encode(st.session_state.generated_pdf_data).decode('utf-8')
                pdf_display = f'<embed src="data:application/pdf;base64,{base64_pdf}" width="700" height="1000" type="application/pdf">'
                st.markdown(pdf_display, unsafe_allow_html=True)
                
                # Add button to generate a new quote
                if st.button("Generate New Quote"):
                    st.session_state.show_pdf_preview = False
                    st.session_state.generated_pdf_data = None
                    st.session_state.need_rerun = True

        else:
            st.info(f"No parts found for this client/VIN. Please add parts first.")

# --- Generate Text Quote Flow ---
elif st.session_state.view == 'generate_text_quote_flow':
    st.header("Generate Quote (Text)")
    if st.button("⬅️ Back to Main"):
        reset_view_and_state()
    
    st.divider()

    df_clients, df_vins, df_parts, df_part_suppliers = load_data()

    # Step 1: Select a Client
    client_options = [''] + sorted(df_clients['phone'].dropna().unique().tolist())
    st.session_state.quote_selected_phone = st.selectbox("Select Client Phone Number:", options=client_options, key="quote_text_phone_selector")

    if st.session_state.quote_selected_phone:
        selected_client_name = df_clients[df_clients['phone'].astype(str) == st.session_state.quote_selected_phone]['client_name'].iloc[0]
        st.subheader(f"Parts for Client: `{selected_client_name}`")

        # Step 2: Allow VIN selection for that client
        vins_for_client = df_vins[df_vins['client_phone'].astype(str) == st.session_state.quote_selected_phone]['vin_number'].tolist()
        vin_options = ['Show All Parts'] + sorted(vins_for_client)
        st.session_state.quote_selected_vin = st.selectbox("Filter by VIN (optional):", options=vin_options, key="quote_text_vin_selector")

        # Step 3: Get parts based on selections
        if st.session_state.quote_selected_vin == 'Show All Parts':
            parts_to_display = df_parts[df_parts['client_phone'].astype(str) == st.session_state.quote_selected_phone]
        else:
            parts_to_display = df_parts[
                (df_parts['client_phone'].astype(str) == st.session_state.quote_selected_phone) &
                (df_parts['vin_number'] == st.session_state.quote_selected_vin)
            ]
        
        if not parts_to_display.empty:
            
            selected_parts_data = {}  # Store part_id -> selected_supplier_index
            
            st.markdown("---")
            st.markdown("### Select Parts and Suppliers to Include in Quote:")
            
            for _, part_row in parts_to_display.iterrows():
                # Get all suppliers for this part
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                
                if not part_suppliers.empty:
                    # Create expander for each part
                    with st.expander(f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']}"):
                        # Display part info
                        st.write(f"**Part:** {part_row['part_name']}")
                        st.write(f"**Number:** {part_row['part_number']}")
                        st.write(f"**Quantity:** {part_row['quantity']}")
                        if pd.notna(part_row['vin_number']):
                            st.write(f"**VIN:** {part_row['vin_number']}")
                        
                        st.markdown("---")
                        st.write("**Available Suppliers:**")
                        
                        # Create radio buttons for supplier selection
                        supplier_options = []
                        supplier_details = []  # Store supplier details for reference
                        for idx, supplier in part_suppliers.iterrows():
                            option_text = f"{supplier['supplier_name']} - ${supplier['selling_price']:.2f} - {supplier['delivery_time'] or 'No delivery time'}"
                            supplier_options.append(option_text)
                            supplier_details.append({
                                'idx': idx,
                                'name': supplier['supplier_name'],
                                'price': supplier['selling_price'],
                                'delivery_time': supplier['delivery_time'] or 'No delivery time'
                            })
                        
                        # Default to first supplier
                        default_idx = 0
                        supplier_choice = st.radio(
                            f"Select supplier for {part_row['part_name']}",
                            options=supplier_options,
                            index=default_idx,
                            key=f"text_supplier_choice_{part_row['id']}"
                        )
                        
                        # Find the selected supplier details
                        selected_index = supplier_options.index(supplier_choice)
                        selected_supplier = supplier_details[selected_index]
                        
                        # Checkbox to include this part
                        include_part = st.checkbox(
                            "Include this part in quote",
                            value=True,
                            key=f"text_include_part_{part_row['id']}"
                        )
                        
                        if include_part:
                            selected_parts_data[part_row['id']] = {
                                'supplier_idx': selected_supplier['idx'],
                                'price': selected_supplier['price'],
                                'delivery_time': selected_supplier['delivery_time'],
                                'supplier_name': selected_supplier['name']
                            }
                else:
                    # For parts without suppliers
                    part_label = f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']} - No suppliers available"
                    if pd.notna(part_row['vin_number']):
                        part_label += f" (VIN: {part_row['vin_number']})"
                    
                    if st.checkbox(part_label, key=f"text_quote_checkbox_{part_row['id']}"):
                        selected_parts_data[part_row['id']] = {
                            'supplier_idx': None,
                            'price': 0.0,
                            'delivery_time': None,
                            'supplier_name': "No supplier"
                        }

            st.markdown("---")
            
            # Delivery time selection (only show if we have delivery times)
            delivery_options_set = set()
            for part_id, supplier_data in selected_parts_data.items():
                if supplier_data['delivery_time'] and supplier_data['delivery_time'].strip():
                    delivery_options_set.add(supplier_data['delivery_time'])

            # Add "IN STOCK" as an option
            delivery_options_set.add("IN STOCK")
            delivery_options = sorted(list(delivery_options_set))

            # Default to "IN STOCK" if available, otherwise first option
            default_delivery_index = delivery_options.index("IN STOCK") if "IN STOCK" in delivery_options else 0

            if len(delivery_options) > 1:
                st.subheader("Select Delivery Time")
                selected_delivery_time = st.selectbox(
                    "Choose the delivery time to display in the quote:",
                    options=delivery_options,
                    index=default_delivery_index,
                    key="delivery_time_selector"
                )
            else:
                selected_delivery_time = delivery_options[0] if delivery_options else "IN STOCK"
            
            # Add option to specify if parts are used or new
            if selected_parts_data:
                st.subheader("Part Condition")
                part_conditions = {}
                
                with st.container():
                    for part_id in selected_parts_data.keys():
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        condition = st.selectbox(
                            f"Condition for {part_row['part_name']}",
                            ["New", "Used", "Refurbished"],
                            key=f"condition_{part_id}"
                        )
                        part_conditions[part_id] = condition
            
            if st.button("Generate Text Quote"):
                if not selected_parts_data:
                    st.warning("Please select at least one part to generate a text quote.")
                else:
                    # Build the text quote string with the correct format
                    quote_text = "*SEE PRICES BELOW*\n\n"
                    
                    # Delivery time display
                    if selected_delivery_time == "IN STOCK":
                        quote_text += "*IN STOCK*\n\n"
                    else:
                        # Extract just the number of days if it's in a phrase
                        import re
                        days_match = re.search(r'(\d+)\s*(day|days|business day|business days)', selected_delivery_time, re.IGNORECASE)
                        if days_match:
                            days = days_match.group(1)
                            quote_text += f"*DELIVERY WITHIN {days} BUSINESS DAYS*\n\n"
                        else:
                            quote_text += f"*DELIVERY WITHIN {selected_delivery_time} BUSINESS DAYS*\n\n"
                    
                    quote_text += "*CASH (At Our Office) OR ONLINE BANK TRANSFER ONLY*\n\n"
                    quote_text += "*Upon Confirmation An Official Quote Will Be Sent With Payment Details.*\n\n"
                    
                    if st.session_state.quote_selected_vin and st.session_state.quote_selected_vin != 'Show All Parts':
                        quote_text += f"*Vin #* {st.session_state.quote_selected_vin}\n\n"
                    
                    part_counter = 1
                    total_amount = 0
                    
                    for part_id, supplier_data in selected_parts_data.items():
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        
                        # Use the stored price instead of querying again
                        selling_price = supplier_data['price']
                        total_amount += (part_row['quantity'] * selling_price)
                        
                        # Get the condition for this part
                        condition = part_conditions.get(part_id, "New")
                        
                        # Format according to your requested pattern
                        quote_text += f"{part_counter}) {part_row['part_name']} - {part_row['quantity']} - ${selling_price:.2f} ({condition.lower()} item)\n"
                        part_counter += 1
                    
                    # Add total amount
                    quote_text += f"\n*TOTAL: ${total_amount:.2f}*"
                    
                    st.session_state.generated_text_quote = quote_text
            
            if st.session_state.generated_text_quote:
                st.markdown("---")
                st.subheader("Copy and Paste Quote")
                st.text_area("Generated Quote", st.session_state.generated_text_quote, height=300)
                
                # Add a copy button
                if st.button("📋 Copy to Clipboard"):
                    st.write("✅ Quote copied to clipboard!")
                
                if st.button("🔄 Clear Quote"):
                    st.session_state.generated_text_quote = ""
                    st.session_state.need_rerun = True

        else:
            st.info("No parts found for this client/VIN. Please add parts first.")

# --- Parts Inventory View ---
elif st.session_state.view == 'view_parts_inventory':
    st.header("Parts Inventory")
    
    if st.button("⬅️ Back to Main"):
        reset_view_and_state()
    
    st.markdown("---")
    
    # Advanced filtering options
    with st.expander("🔍 Advanced Filters", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            filter_name = st.text_input("Filter by Part Name")
            filter_quantity_min = st.number_input("Min Quantity", min_value=0, value=0)
        
        with col2:
            filter_number = st.text_input("Filter by Part Number")
            filter_quantity_max = st.number_input("Max Quantity", min_value=0, value=1000)
        
        with col3:
            filter_client_phone = st.text_input("Filter by Client Phone")
            filter_has_vin = st.selectbox("Has VIN?", ["All", "With VIN", "Without VIN"])
    
    # Search box
    search_query = st.text_input("Quick Search (name, number, or notes)")
    
    # Apply filters
    filtered_parts = df_parts.copy()
    
    # Text filters
    if search_query:
        filtered_parts = filtered_parts[
            filtered_parts['part_name'].str.contains(search_query, case=False, na=False) |
            filtered_parts['part_number'].str.contains(search_query, case=False, na=False) |
            filtered_parts['notes'].str.contains(search_query, case=False, na=False)
        ]
    
    if filter_name:
        filtered_parts = filtered_parts[filtered_parts['part_name'].str.contains(filter_name, case=False, na=False)]
    
    if filter_number:
        filtered_parts = filtered_parts[filtered_parts['part_number'].str.contains(filter_number, case=False, na=False)]
    
    if filter_client_phone:
        filtered_parts = filtered_parts[filtered_parts['client_phone'] == filter_client_phone]
    
    # Quantity range filter
    filtered_parts = filtered_parts[
        (filtered_parts['quantity'] >= filter_quantity_min) & 
        (filtered_parts['quantity'] <= filter_quantity_max)
    ]
    
    # VIN filter
    if filter_has_vin == "With VIN":
        filtered_parts = filtered_parts[filtered_parts['vin_number'].notna()]
    elif filter_has_vin == "Without VIN":
        filtered_parts = filtered_parts[filtered_parts['vin_number'].isna()]
    
    # Display results with pagination
    if not filtered_parts.empty:
        # Parts pagination
        if 'parts_page' not in st.session_state:
            st.session_state.parts_page = 0
        
        PARTS_PER_PAGE = 15
        total_parts = len(filtered_parts)
        total_pages = (total_parts + PARTS_PER_PAGE - 1) // PARTS_PER_PAGE
        current_page = st.session_state.parts_page
        
        start_idx = current_page * PARTS_PER_PAGE
        end_idx = min(start_idx + PARTS_PER_PAGE, total_parts)
        current_parts = filtered_parts.iloc[start_idx:end_idx]
        
        st.write(f"**Found {total_parts} parts**")
        
        # Pagination controls
        if total_pages > 1:
            col_prev, col_info, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button("◀ Previous Page", key="parts_prev", disabled=current_page == 0):
                    st.session_state.parts_page -= 1
                    st.session_state.need_rerun = True
            with col_info:
                st.write(f"Page {current_page + 1} of {total_pages}")
            with col_next:
                if st.button("Next Page ▶", key="parts_next", disabled=end_idx >= total_parts):
                    st.session_state.parts_page += 1
                    st.session_state.need_rerun = True
        
        # Display parts
        for _, part_row in current_parts.iterrows():
            with st.expander(f"{part_row['part_name']} ({part_row['part_number']}) - Qty: {part_row['quantity']}"):
                st.subheader("Part Information")
                st.write(f"**Part ID:** {part_row['id']}")
                st.write(f"**Quantity:** {part_row['quantity']}")
                st.write(f"**Notes:** {part_row['notes']}")
                st.write(f"**Client Phone:** {part_row['client_phone']}")
                st.write(f"**VIN:** {part_row['vin_number'] if pd.notna(part_row['vin_number']) else 'Not assigned'}")
                
                st.markdown("---")
                st.subheader("Supplier Information")
                
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                
                if not part_suppliers.empty:
                    st.dataframe(part_suppliers[['supplier_name', 'buying_price', 'selling_price', 'delivery_time']], 
                                use_container_width=True, hide_index=True)
                else:
                    st.info("No suppliers found for this part.")
        
        # Reset pagination when filters change
        if st.button("Clear Filters"):
            st.session_state.parts_page = 0
            st.session_state.need_rerun = True
            
    else:
        st.info("No parts found matching your search criteria.")

# --- CLIENT LIST VIEW ---
elif st.session_state.view == 'client_list':
    st.header("All Clients")
    if st.button("⬅️ Back to Main"):
        reset_view_and_state()

    st.divider()

    # Pagination settings
    CLIENTS_PER_PAGE = 10

    if not df_clients.empty:
        # Calculate pagination
        total_clients = len(df_clients)
        total_pages = (total_clients + CLIENTS_PER_PAGE - 1) // CLIENTS_PER_PAGE
        current_page = st.session_state.clients_page
        
        start_idx = current_page * CLIENTS_PER_PAGE
        end_idx = min(start_idx + CLIENTS_PER_PAGE, total_clients)
        current_clients = df_clients.iloc[start_idx:end_idx]

        # Pagination controls
        col_prev, col_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button("◀ Previous", disabled=current_page == 0):
                st.session_state.clients_page -= 1
                st.session_state.need_rerun = True
        with col_info:
            st.write(f"Page {current_page + 1} of {total_pages} | Showing {start_idx + 1}-{end_idx} of {total_clients} clients")
        with col_next:
            if st.button("Next ▶", disabled=end_idx >= total_clients):
                st.session_state.clients_page += 1
                st.session_state.need_rerun = True

        st.markdown("---")

        # Client list
        for index, client_row in current_clients.iterrows():
            col1, col2, col3 = st.columns([0.4, 0.3, 0.3])
            with col1:
                st.write(f"**{client_row['client_name']}**")
            with col2:
                st.write(client_row['phone'])
            with col3:
                action_col1, action_col2 = st.columns([1, 1])
                with action_col1:
                    if st.button("View Details", key=f"view_details_{client_row['phone']}"):
                        st.session_state.view = 'client_details'
                        st.session_state.current_client_phone = client_row['phone']
                        st.session_state.current_client_name = client_row['client_name']
                        st.session_state.need_rerun = True
                with action_col2:
                    delete_client_key = f"delete_client_{client_row['phone']}"
                    if st.button("❌", help="Delete Client", key=delete_client_key):
                        if st.session_state.get(f'confirm_delete_client_{client_row["phone"]}', False):
                            with st.spinner("Deleting client..."):
                                delete_client(client_row['phone'], st.session_state.username)

                            st.success(f"Client `{client_row['client_name']}` and all associated data deleted.")
                            st.session_state.pop(f'confirm_delete_client_{client_row["phone"]}', None)
                            st.cache_data.clear()
                            st.session_state.need_rerun = True
                        else:
                            st.session_state[f'confirm_delete_client_{client_row["phone"]}'] = True
                            st.warning("Are you sure? Click again to confirm.")
                    if st.session_state.get(f'confirm_delete_client_{client_row["phone"]}', False) and st.button("Cancel", key=f'cancel_delete_client_{client_row["phone"]}'):
                        st.session_state.pop(f'confirm_delete_client_{client_row["phone"]}', None)
                        st.session_state.need_rerun = True
            st.markdown("---")
    else:
        st.info("No clients registered yet.")

# --- CLIENT DETAILS VIEW ---
elif st.session_state.view == 'client_details':
    st.header("Client Details")
    
    if not st.session_state.edit_mode:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
        with col1:
            if st.button("Back to Main"):
                reset_view_and_state()
        with col2:
            if st.button("Edit Client"):
                st.session_state.edit_mode = True
                st.session_state.need_rerun = True
        with col3:
            if st.button("Add VIN"):
                st.session_state.view = 'add_vin_to_existing_client'
                st.session_state.need_rerun = True
        with col4:
            if st.button("Add Part to Existing VIN"):
                st.session_state.selected_vin_to_add_part = None
                st.session_state.view = 'add_part_for_client'
                st.session_state.need_rerun = True
        with col5:
            if st.button("Add Part Without VIN"):
                st.session_state.view = 'add_part_without_vin_for_client'
                st.session_state.need_rerun = True

        st.divider()
        
        if st.session_state.current_client_phone:
            st.subheader(f"Phone: {st.session_state.current_client_phone}")
            
            client_name_to_display = st.session_state.current_client_name if st.session_state.current_client_name else "No name provided"
            st.write(f"**Client Name:** {client_name_to_display}")
            
            unassigned_parts_for_client = df_parts[
                (df_parts['client_phone'] == st.session_state.current_client_phone) & 
                (df_parts['vin_number'].isnull())
            ]
            
            if not unassigned_parts_for_client.empty:
                st.write("### Unassigned Parts for this Client")
                for index, part_row in unassigned_parts_for_client.iterrows():
                    col_part1, col_part2, col_part3, col_part4 = st.columns([0.3, 0.3, 0.2, 0.2])
                    with col_part1:
                        st.write(f"**Name:** {part_row['part_name']}")
                    with col_part2:
                        st.write(f"**Number:** {part_row['part_number']}")
                    with col_part3:
                        delete_part_key = f"delete_part_unassigned_{part_row['id']}"
                        if st.button("🗑️", key=delete_part_key):
                            with st.spinner("Deleting part..."):
                                delete_part(part_row['id'])
                            st.cache_data.clear()
                            st.session_state.need_rerun = True
                    with col_part4:
                        if st.button("✏️", key=f"edit_part_unassigned_{part_row['id']}"):
                            st.session_state.view = 'edit_part'
                            st.session_state.part_to_edit_id = part_row['id']
                            st.session_state.need_rerun = True

                    # Display suppliers for this unassigned part
                    part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                    if not part_suppliers.empty:
                        with st.expander("View Suppliers"):
                            for _, supplier_row in part_suppliers.iterrows():
                                st.write(f"- **Supplier:** {supplier_row['supplier_name']} | **Buying Price:** ${supplier_row['buying_price']:.2f} | **Selling Price:** ${supplier_row['selling_price']:.2f}")

                st.markdown("---")
            else:
                st.info("No unassigned parts for this client.")

            client_vins = df_vins[df_vins['client_phone'].astype(str) == str(st.session_state.current_client_phone)]
            
            if not client_vins.empty:
                st.write("### Registered VINs")
                
                for index, vin_row in client_vins.iterrows():
                    col_vin_display, col_vin_actions = st.columns([0.8, 0.2])
                    with col_vin_display:
                        st.markdown(f"**VIN Number:** `{vin_row['vin_number']}`")
                        st.markdown(f"**Model:** {vin_row['model']}")
                        st.markdown(f"**Prod. Yr:** {vin_row['prod_yr']}")
                        st.markdown(f"**Body:** {vin_row['body']}")
                        st.markdown(f"**Engine:** {vin_row['engine']}")
                        st.markdown(f"**Code:** {vin_row['code']}")
                        st.markdown(f"**Transmission:** {vin_row['transmission']}")

                    with col_vin_actions:
                        delete_vin_key = f'delete_vin_{vin_row["vin_number"]}'
                        if st.button("❌ Delete VIN", key=delete_vin_key):
                            if st.session_state.get(f'confirm_delete_vin_{vin_row["vin_number"]}', False):
                                with st.spinner("Deleting VIN..."):
                                    delete_vin(vin_row['vin_number'], st.session_state.username)

                                st.success(f"VIN `{vin_row['vin_number']}` and all associated parts deleted.")
                                st.session_state.pop(f'confirm_delete_vin_{vin_row["vin_number"]}', None)
                                st.cache_data.clear()
                                st.session_state.need_rerun = True
                            else:
                                st.session_state[f'confirm_delete_vin_{vin_row["vin_number"]}'] = True
                                st.warning("Are you sure? Click again to confirm.")
                        if st.session_state.get(f'confirm_delete_vin_{vin_row["vin_number"]}', False) and st.button("Cancel", key=f'cancel_delete_{vin_row["vin_number"]}'):
                            st.session_state.pop(f'confirm_delete_vin_{vin_row["vin_number"]}', None)
                            st.session_state.need_rerun = True

                        if st.button(f"Add Part", key=f'add_part_vin_{vin_row["vin_number"]}'):
                            st.session_state.selected_vin_to_add_part = vin_row['vin_number']
                            st.session_state.view = 'add_part_for_client'
                            st.session_state.need_rerun = True

                    parts_for_vin = df_parts[df_parts['vin_number'] == vin_row['vin_number']]
                    if not parts_for_vin.empty:
                        with st.expander(f"View Parts for VIN {vin_row['vin_number']}"):
                            for part_index, part_row in parts_for_vin.iterrows():
                                col_part1, col_part2, col_part3 = st.columns([0.5, 0.2, 0.2])
                                with col_part1:
                                    st.write(f"**Name:** {part_row['part_name']}")
                                with col_part2:
                                    st.write(f"**Number:** {part_row['part_number']}")
                                with col_part3:
                                    delete_part_key = f"delete_part_{part_row['id']}"
                                    if st.button("🗑️", key=delete_part_key):
                                        with st.spinner("Deleting part..."):
                                            delete_part(part_row['id'], st.session_state.username)

                                        st.cache_data.clear()
                                        st.session_state.need_rerun = True
                                if st.button("✏️", key=f"edit_part_{part_row['id']}"):
                                    st.session_state.view = 'edit_part'
                                    st.session_state.part_to_edit_id = part_row['id']
                                    st.session_state.need_rerun = True
                                
                                # Display suppliers for this part
                                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                                if not part_suppliers.empty:
                                    with st.expander("View Suppliers"):
                                        for _, supplier_row in part_suppliers.iterrows():
                                            st.write(f"- **Supplier:** {supplier_row['supplier_name']} | **Buying Price:** ${supplier_row['buying_price']:.2f} | **Selling Price:** ${supplier_row['selling_price']:.2f}")

                            st.markdown("---")
                    else:
                        st.info(f"No parts registered for VIN {vin_row['vin_number']} .")
                        
                    st.markdown("---")
            else:
                st.info("No VINs registered for this client.")
    
    else:
        st.subheader("Edit Client Information")
        with st.form("edit_client_form"):
            new_phone = st.text_input("New Phone", value=str(st.session_state.current_client_phone))
            new_name = st.text_input("New Client Name", value=st.session_state.current_client_name)
            
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                if st.form_submit_button("Save Changes"):
                    old_phone = str(st.session_state.current_client_phone)
                    with st.spinner("Updating client..."):
                        update_client_and_vins(old_phone, new_phone, new_name)
                    st.success("✅ Client and associated VINs updated successfully!")
                    st.session_state.edit_mode = False
                    st.cache_data.clear()
                    st.session_state.need_rerun = True
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.edit_mode = False
                    st.session_state.need_rerun = True
            with col3:
                if st.form_submit_button("View VINs"):
                    st.session_state.edit_mode = False
                    st.session_state.need_rerun = True

# --- EDIT PART VIEW ---
elif st.session_state.view == 'edit_part':
    st.header("Edit Part Details")
    
    part_id = st.session_state.part_to_edit_id
    part_to_edit = df_parts[df_parts['id'] == part_id].iloc[0]
    
    # Get existing suppliers
    existing_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_id]
    
    # Initialize supplier count for the form
    if 'supplier_count_edit' not in st.session_state or st.session_state.part_to_edit_id != st.session_state.get('last_edit_id'):
        st.session_state.supplier_count_edit = max(1, len(existing_suppliers))
        st.session_state.last_edit_id = st.session_state.part_to_edit_id

    def add_supplier_field_edit():
        st.session_state.supplier_count_edit += 1
    
    def remove_supplier_field_edit():
        if st.session_state.supplier_count_edit > 1:
            st.session_state.supplier_count_edit -= 1

    col1, col2 = st.columns([1, 1])
    with col1:
        st.button("Add another supplier", on_click=add_supplier_field_edit, key="add_supplier_edit_button")
    with col2:
        if st.session_state.supplier_count_edit > 1:
            st.button("Remove last supplier", on_click=remove_supplier_field_edit, key="remove_supplier_edit_button")

    with st.form("edit_part_form"):
        st.write(f"Editing Part ID: **{part_id}**")
        
        part_name = st.text_input("Part Name", value=part_to_edit['part_name'])
        part_number = st.text_input("Part Number", value=part_to_edit['part_number'])
        quantity = st.number_input("Quantity", min_value=1, value=int(part_to_edit['quantity']))
        notes = st.text_area("Notes", value=part_to_edit['notes'] or "")

        st.markdown("---")
        st.markdown("### Supplier Information")
        
        suppliers_data = []
        
        # Loop to create supplier fields based on count
        for i in range(st.session_state.supplier_count_edit):
            st.subheader(f"Supplier {i+1}")
            
            # Use existing data if available
            if i < len(existing_suppliers):
                supplier_row = existing_suppliers.iloc[i]
                supplier_name = st.text_input("Supplier Name", value=supplier_row['supplier_name'], key=f"supplier_name_edit_{i}")
                buying_price = st.number_input("Buying Price ($)", min_value=0.0, value=supplier_row['buying_price'] or 0.0, format="%.2f", key=f"buying_price_edit_{i}")
                selling_price = st.number_input("Selling Price ($)", min_value=0.0, value=supplier_row['selling_price'] or 0.0, format="%.2f", key=f"selling_price_edit_{i}")
                delivery_time = st.text_input("Delivery Time", value=supplier_row['delivery_time'] or "", key=f"delivery_time_edit_{i}")
            else:
                supplier_name = st.text_input("Supplier Name", key=f"supplier_name_edit_{i}")
                buying_price = st.number_input("Buying Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"buying_price_edit_{i}")
                selling_price = st.number_input("Selling Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"selling_price_edit_{i}")
                delivery_time = st.text_input("Delivery Time", key=f"delivery_time_edit_{i}")

            suppliers_data.append({
                "name": supplier_name,
                "buying_price": buying_price,
                "selling_price": selling_price,
                "delivery_time": delivery_time,
            })
        
        col1, col2 = st.columns(2)
        with col1:
            if st.form_submit_button("Save Changes"):
                with st.spinner("Updating part..."):
                    update_part(part_id, part_name, part_number, quantity, notes, suppliers_data, st.session_state.username)
                st.success("✅ Part updated successfully!")
                st.session_state.view = 'client_details'
                st.session_state.part_to_edit_id = None
                st.session_state.supplier_count_edit = 1
                st.cache_data.clear()
                st.session_state.need_rerun = True
        with col2:
            if st.form_submit_button("Cancel"):
                st.session_state.view = 'client_details'
                st.session_state.part_to_edit_id = None
                st.session_state.supplier_count_edit = 1
                st.session_state.need_rerun = True

# --- VIN DETAILS VIEW ---
elif st.session_state.view == 'vin_details':
    st.header(f"VIN Details: {st.session_state.current_vin_no_view}")
    
    if st.button("Back to Client Details"):
        st.session_state.view = 'client_details'
        st.session_state.need_rerun = True
    
    st.divider()

    selected_vin_data = df_vins[df_vins['vin_number'] == st.session_state.current_vin_no_view]
    
    if not selected_vin_data.empty:
        st.write("### Vehicle Information")
        st.dataframe(selected_vin_data, use_container_width=True, hide_index=True)
        st.divider()

        st.write("### Registered Parts")
        parts_for_vin = df_parts[df_parts['vin_number'] == st.session_state.current_vin_no_view]
        
        if not parts_for_vin.empty:
            st.dataframe(parts_for_vin, use_container_width=True, hide_index=True)
        else:
            st.info("No parts registered for this VIN.")
    else:
        st.warning("VIN details not found.")

# --- ADD PART FLOW (GENERAL) ---
elif st.session_state.view in ['add_part_to_existing_vin', 'add_part_without_vin_flow', 'add_part_without_vin_for_client', 'add_part_for_client']:

    # Function to reset session state for a new part entry
    def reset_part_entry():
        st.session_state.part_count = 1
        st.session_state.supplier_count = 1
        st.session_state.current_part_id_to_add_supplier = None
        st.session_state.generated_quote_msg = ""
        st.session_state.last_part_ids = []

    def render_part_forms():
    # Initialize session state for current part management
        if 'current_part_management' not in st.session_state:
            st.session_state.current_part_management = {
                'current_part_index': 0,
                'parts_data': [],
                'saved_part_ids': []
            }
        
        current_mgmt = st.session_state.current_part_management
        
        # Tab 1: Add Parts
        with st.expander("➕ Add Parts", expanded=len(current_mgmt['saved_part_ids']) == 0):
            st.subheader("Add New Parts")
            
            with st.form("add_multiple_parts_form", clear_on_submit=False):
                parts_data = []
                for i in range(st.session_state.part_count):
                    st.markdown(f"**Part {i+1}**")
                    part_name = st.text_input("Part Name*", key=f"part_name_{i}", help="At least name or number is required")
                    part_number = st.text_input("Part Number", key=f"part_number_{i}")
                    quantity = st.number_input("Quantity*", min_value=1, value=1, key=f"quantity_{i}")
                    notes = st.text_area("Notes", key=f"notes_{i}")
                    st.markdown("---")
                    
                    parts_data.append({
                        "name": part_name,
                        "number": part_number,
                        "quantity": quantity,
                        "notes": notes,
                    })
                
                col1, col2 = st.columns(2)
                with col1:
                    save_parts_btn = st.form_submit_button("💾 Save Parts")
                with col2:
                    cancel_btn = st.form_submit_button("❌ Cancel")
                
                if save_parts_btn:
                    all_valid = True
                    validation_errors = []
                    for idx, part in enumerate(parts_data):
                        if not part["name"] and not part["number"]:
                            all_valid = False
                            validation_errors.append(f"Part {idx+1}: Name or Number is required")
                    
                    if all_valid:
                        saved_ids = []
                        with st.spinner("Saving parts..."):
                            for part in parts_data:
                                try:
                                    if st.session_state.view in ['add_part_to_existing_vin', 'add_part_for_client']:
                                        # Safely get client phone from VIN
                                        vin_match = df_vins[df_vins['vin_number'] == st.session_state.selected_vin_to_add_part]
                                        if vin_match.empty:
                                            st.error(f"VIN {st.session_state.selected_vin_to_add_part} not found in database")
                                            continue
                        
                                        client_phone = vin_match['client_phone'].iloc[0]
                                        part_id = safe_add_part_to_vin(
                                            st.session_state.selected_vin_to_add_part,
                                            client_phone,
                                            part,
                                            [], # No suppliers added at this stage
                                            st.session_state.username  # ADDED USERNAME PARAMETER
                                        )
                                    elif st.session_state.view == 'add_part_without_vin_flow':
                                        part_id = add_part_without_vin(
                                            part["name"],
                                            part["number"],
                                            part["quantity"],
                                            part["notes"],
                                            client_phone=None,
                                            suppliers=[],
                                            username=st.session_state.username
                                        )
                                    elif st.session_state.view == 'add_part_without_vin_for_client':
                                        part_id = add_part_without_vin(
                                            part["name"],
                                            part["number"],
                                            part["quantity"],
                                            part["notes"],
                                            client_phone=st.session_state.current_client_phone,
                                            suppliers=[],
                                            username=st.session_state.username
                                        )
                    
                                    if part_id:
                                        saved_ids.append(part_id)
                                    else:
                                        st.error(f"Failed to save part: {part.get('name', 'Unknown')}")
                            
                                except Exception as e:
                                    st.error(f"Error saving part '{part.get('name', 'Unknown')}': {str(e)}")
                                    continue
                        
                        if saved_ids:
                            st.session_state.current_part_management = {
                                'current_part_index': 0,
                                'parts_data': parts_data,
                                'saved_part_ids': saved_ids
                            }
                            st.session_state.current_part_id_to_add_supplier = saved_ids[0]
                            st.success(f"✅ {len(saved_ids)} part(s) saved successfully!")
                            st.info("Please add supplier information for each part using the 'Manage Suppliers' section below.")
                            st.cache_data.clear()
                            st.session_state.need_rerun = True
                        else:
                            st.error("No parts were saved successfully")
                    else:
                        for error in validation_errors:
                            st.warning(error)
                
                if cancel_btn:
                    reset_view_and_state()
        
        # Tab 2: Manage Suppliers (only show if parts were saved)
        if current_mgmt['saved_part_ids']:
            st.markdown("---")
            st.subheader("🛠️ Manage Suppliers")
            
            # Part navigation
            if len(current_mgmt['saved_part_ids']) > 1:
                st.write(f"**Managing Part {current_mgmt['current_part_index'] + 1} of {len(current_mgmt['saved_part_ids'])}**")
                col_nav1, col_nav2, col_nav3 = st.columns(3)
                with col_nav1:
                    if st.button("⬅️ Previous Part", disabled=current_mgmt['current_part_index'] == 0):
                        st.session_state.current_part_management['current_part_index'] -= 1
                        st.session_state.current_part_id_to_add_supplier = current_mgmt['saved_part_ids'][st.session_state.current_part_management['current_part_index']]
                        st.session_state.need_rerun = True
                with col_nav2:
                    st.write(f"Part {current_mgmt['current_part_index'] + 1}")
                with col_nav3:
                    if st.button("Next Part ➡️", disabled=current_mgmt['current_part_index'] >= len(current_mgmt['saved_part_ids']) - 1):
                        st.session_state.current_part_management['current_part_index'] += 1
                        st.session_state.current_part_id_to_add_supplier = current_mgmt['saved_part_ids'][st.session_state.current_part_management['current_part_index']]
                        st.session_state.need_rerun = True
            
            # Current part info
            current_part_id = current_mgmt['saved_part_ids'][current_mgmt['current_part_index']]
            part_info = df_parts[df_parts['id'] == current_part_id]
            
            if not part_info.empty:
                part_row = part_info.iloc[0]
                st.info(f"**Current Part:** {part_row['part_name']} ({part_row['part_number']}) - Qty: {part_row['quantity']}")
                
                # Load existing suppliers for this part
                existing_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == current_part_id]
                
                if not existing_suppliers.empty:
                    st.subheader("Current Suppliers")
                    for index, supplier in existing_suppliers.iterrows():
                        with st.expander(f"Supplier: {supplier['supplier_name']}"):
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(f"**Buying Price:** ${supplier['buying_price']:.2f}")
                                st.write(f"**Selling Price:** ${supplier['selling_price']:.2f}")
                                st.write(f"**Delivery Time:** {supplier['delivery_time']}")
                            with col2:
                                if st.button("✏️ Edit", key=f"edit_supplier_{supplier['id']}"):
                                    st.session_state.editing_supplier_id = supplier['id']
                                    st.session_state.edit_supplier_name = supplier['supplier_name']
                                    st.session_state.edit_buying_price = supplier['buying_price']
                                    st.session_state.edit_selling_price = supplier['selling_price']
                                    st.session_state.edit_delivery_time = supplier['delivery_time']
                                
                                if st.button("🗑️ Delete", key=f"delete_supplier_{supplier['id']}"):
                                    st.session_state.deleting_supplier_id = supplier['id']
                    
                    st.markdown("---")
                
                # Add new supplier form
                st.subheader("Add New Supplier")
                
                with st.form("add_supplier_form", clear_on_submit=True):
                    supplier_name = st.text_input("Supplier Name*", help="Required field", key=f"supplier_name_{current_part_id}")
                    buying_price = st.number_input("Buying Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"buying_price_{current_part_id}")
                    selling_price = st.number_input("Selling Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"selling_price_{current_part_id}")
                    delivery_time = st.text_input("Delivery Time", key=f"delivery_time_{current_part_id}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        add_supplier_btn = st.form_submit_button("➕ Add Supplier")
                    with col2:
                        cancel_supplier_btn = st.form_submit_button("Cancel")
                    
                    if add_supplier_btn:
                        if not supplier_name:
                            st.warning("Supplier name is required")
                        else:
                            try:
                                add_supplier_to_part(current_part_id, supplier_name, buying_price, selling_price, delivery_time, st.session_state.username)
                                st.success("✅ Supplier added successfully!")
                                st.cache_data.clear()
                                st.session_state.need_rerun = True
                            except Exception as e:
                                st.error(f"Error adding supplier: {str(e)}")
                
                # Action buttons after adding suppliers
                st.markdown("---")
                st.subheader("Next Steps")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("➕ Add Another Part", help="Add more parts to this client/VIN"):
                        reset_part_management()
                        st.session_state.need_rerun = True
                
                with col2:
                    if st.button("🏠 Back to Client", help="Return to client details"):
                        st.session_state.view = 'client_details'
                        st.session_state.need_rerun = True
                
                with col3:
                    if st.button("📋 View All Parts", help="View all parts for this client"):
                        st.session_state.view = 'view_parts_inventory'
                        st.session_state.need_rerun = True
        
        else:
            # Only show this message if no parts have been saved yet
            if not current_mgmt['saved_part_ids']:
                st.info("Please add and save parts first to manage suppliers.")
                
    # --- Call render_part_forms() for the 'add_part' views ---
if st.session_state.view in ['add_part_to_existing_vin', 'add_part_without_vin_flow', 'add_part_without_vin_for_client', 'add_part_for_client']:
    if st.session_state.view == 'add_part_to_existing_vin':
        st.header(f"Add Part to VIN: {st.session_state.selected_vin_to_add_part}")
    elif st.session_state.view == 'add_part_without_vin_flow':
        st.header("Add Part Without VIN")
    elif st.session_state.view == 'add_part_for_client':
        st.header(f"Add Part for Client: {st.session_state.current_client_name}")
        if st.session_state.selected_vin_to_add_part:
            st.write(f"**VIN:** {st.session_state.selected_vin_to_add_part}")
    elif st.session_state.view == 'add_part_without_vin_for_client':
        st.header(f"Add Part for Client: {st.session_state.current_client_name} (No VIN)")

    # Back button
    if st.button("⬅️ Back"):
        if st.session_state.view in ['add_part_without_vin_flow', 'add_part_to_existing_vin']:
            st.session_state.view = 'main'
        else:
            st.session_state.view = 'client_details'
        st.session_state.need_rerun = True

    st.markdown("---")
    render_part_forms()

# --- ADD VIN TO EXISTING CLIENT FLOW ---
elif st.session_state.view == 'add_vin_to_existing_client':
    st.header("Add VIN for Existing Client")
    st.write(f"Client: **{st.session_state.current_client_name}** (Phone: {st.session_state.current_client_phone})")

    if not st.session_state.vin_added:
        with st.form("add_vin_form", clear_on_submit=True):
            vin_no = st.text_input("VIN Number")
            col1, col2 = st.columns([1, 1])
            with col1:
                submitted_vin = st.form_submit_button("Continue")
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.view = 'client_details'
                    st.session_state.need_rerun = True
            
            if submitted_vin:
                if vin_no and not validate_vin(vin_no):
                    st.error("Please enter a valid VIN (13-17 alphanumeric characters) or leave blank")
                else:
                    st.session_state.vin_added = True
                    st.session_state.current_vin_no = vin_no
                    st.session_state.need_rerun = True

    else:
        st.header("Add VIN Details")
        with st.form("add_vin_details_form", clear_on_submit=True):
            st.write(f"Add details for VIN **{st.session_state.current_vin_no}** for client **{st.session_state.current_client_name}** (Phone: {st.session_state.current_client_phone})")
            
            model = st.text_input("Model")
            prod_yr = st.text_input("Prod. Yr")
            body = st.text_input("Body")
            engine = st.text_input("Engine")
            code = st.text_input("Code")
            transmission = st.text_input("Transmission")

            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                submitted_details = st.form_submit_button("Save VIN Details")
            with col2:
                submitted_and_add_part = st.form_submit_button("Save Details & Add Part")
            with col3:
                if st.form_submit_button("Cancel"):
                    st.session_state.view = 'client_details'
                    st.session_state.vin_added = False
                    st.session_state.need_rerun = True
            
            if submitted_details or submitted_and_add_part:
                vin_to_save = st.session_state.current_vin_no if st.session_state.current_vin_no else None
                
                try:
                    # Clean the VIN if it's not empty
                    if vin_to_save:
                        clean_vin = ''.join(vin_to_save.split()).upper()
                        if not validate_vin(clean_vin):
                            st.error("Invalid VIN format. Must be 7, 13, or 17 alphanumeric characters, or empty.")
                            st.stop()
                    else:
                        clean_vin = None
                    
                    with st.spinner("Saving VIN details..."):
                        add_vin_to_client(str(st.session_state.current_client_phone), clean_vin, model, prod_yr, body, engine, code, transmission, st.session_state.username)
                    st.success(f"✅ VIN details saved for {clean_vin if clean_vin else 'No VIN'}!")
                    
                    if submitted_and_add_part:
                        st.session_state.selected_vin_to_add_part = clean_vin if clean_vin else "No VIN provided"
                        st.session_state.view = 'add_part_for_client'
                    else:
                        st.session_state.view = 'client_details'
                    
                    st.session_state.vin_added = False
                    st.session_state.current_vin_no = None
                    st.cache_data.clear()
                    st.session_state.need_rerun = True
                    
                except ValueError as e:
                    st.error(str(e))

# --- SEQUENTIAL CLIENT AND VIN REGISTRATION FLOW ---
elif st.session_state.view == 'add_client':
    if not st.session_state.client_added:
        st.header("Register New Client")
        with st.form("new_client_form", clear_on_submit=True):
            phone = st.text_input("Phone*", help="Required field (7-15 digits)")
            client_name = st.text_input("Client Name")
            
            col1, col2 = st.columns([1,1])
            with col1:
                submitted = st.form_submit_button("Add Client")
            with col2:
                if st.form_submit_button("Back to Main"):
                    reset_view_and_state()
            
            if submitted:
                if phone:
                    if not validate_phone(phone):
                        st.error("Please enter a valid phone number (7-15 digits)")
                    else:
                        try:
                            with st.spinner("Adding new client..."):
                                add_new_client(str(phone), client_name, st.session_state.username)
                            st.session_state.client_added = True
                            st.session_state.current_client_phone = str(phone)
                            st.session_state.current_client_name = client_name
                            st.cache_data.clear()
                            st.session_state.need_rerun = True
                        except ValueError as e:
                            st.error(str(e))
                else:
                    st.warning("Please enter a valid phone number.")

    elif not st.session_state.vin_added:
        st.header("Add VIN for Client")
        with st.form("add_vin_form", clear_on_submit=True):
            st.write(f"Add VIN for: **{st.session_state.current_client_name}** (Phone: {st.session_state.current_client_phone})")
            vin_no = st.text_input("VIN Number", help="Optional: 13-17 alphanumeric characters or leave blank")
            
            col1, col2 = st.columns([1, 1])
            with col1:
                submitted_vin = st.form_submit_button("Continue")
            with col2:
                if st.form_submit_button("Skip and Go to Client Details"):
                    st.session_state.view = 'client_details'
                    st.session_state.client_added = False
                    st.session_state.need_rerun = True
            
            if submitted_vin:
                if vin_no and not validate_vin(vin_no):
                    st.error("Please enter a valid VIN (13-17 alphanumeric characters) or leave blank")
                else:
                    st.session_state.vin_added = True
                    st.session_state.current_vin_no = vin_no
                    st.session_state.need_rerun = True
    
    else:
        st.header("Add VIN Details")
        with st.form("add_vin_details_form", clear_on_submit=True):
            st.write(f"Add details for VIN **{st.session_state.current_vin_no}** for client **{st.session_state.current_client_name}** (Phone: {st.session_state.current_client_phone})")
            
            model = st.text_input("Model")
            prod_yr = st.text_input("Prod. Yr")
            body = st.text_input("Body")
            engine = st.text_input("Engine")
            code = st.text_input("Code")
            transmission = st.text_input("Transmission")

            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                submitted_details = st.form_submit_button("Save VIN Details")
            with col2:
                submitted_and_add_part = st.form_submit_button("Save Details & Add Part")
            with col3:
                if st.form_submit_button("Cancel"):
                    st.session_state.view = 'client_details'
                    st.session_state.vin_added = False
                    st.session_state.need_rerun = True
            
            if submitted_details or submitted_and_add_part:
                vin_to_save = st.session_state.current_vin_no if st.session_state.current_vin_no else None
                
                try:
                    # Clean the VIN if it's not empty
                    if vin_to_save:
                        clean_vin = ''.join(vin_to_save.split()).upper()
                        if not validate_vin(clean_vin):
                            st.error("Invalid VIN format. Must be 7, 13, or 17 alphanumeric characters, or empty.")
                            st.stop()
                    else:
                        clean_vin = None
                    
                    with st.spinner("Saving VIN details..."):
                        # FIXED: Use st.session_state.current_client_phone instead of undefined 'phone' variable
                        add_vin_to_client(
                            str(st.session_state.current_client_phone),  # FIXED HERE
                            clean_vin,
                            model,
                            prod_yr,
                            body,
                            engine,
                            code,
                            transmission,
                            st.session_state.username
                        )
                    st.success(f"✅ VIN details saved for {clean_vin if clean_vin else 'No VIN'}!")
                    
                    if submitted_and_add_part:
                        st.session_state.selected_vin_to_add_part = clean_vin if clean_vin else "No VIN provided"
                        st.session_state.view = 'add_part_for_client'
                    else:
                        st.session_state.view = 'client_details'
                    
                    st.session_state.vin_added = False
                    st.session_state.current_vin_no = None
                    st.cache_data.clear()
                    st.session_state.need_rerun = True
                    
                except ValueError as e:
                    st.error(str(e))

# --- SEARCH RESULTS VIEW ---
elif st.session_state.view == 'search_results':
    st.header("Search Results")
    if st.button("⬅️ Back to Main"):
        reset_view_and_state()
    
    search_results = st.session_state.get('search_results', {})
    
    if search_results:
        if not search_results['clients'].empty:
            st.subheader("Clients")
            st.dataframe(search_results['clients'][['client_name', 'phone']], use_container_width=True)
        
        if not search_results['vins'].empty:
            st.subheader("VINs")
            st.dataframe(search_results['vins'][['vin_number', 'model', 'client_phone']], use_container_width=True)
        
        if not search_results['parts'].empty:
            st.subheader("Parts")
            st.dataframe(search_results['parts'][['part_name', 'part_number', 'quantity', 'client_phone', 'vin_number']], use_container_width=True)
        
        if search_results['clients'].empty and search_results['vins'].empty and search_results['parts'].empty:
            st.info("No results found for your search.")
    else:
        st.info("No search results to display.")

if __name__ == "__main__":
    # This ensures proper execution
    if st.session_state.get('initialized', False):
        st.write("Brent J. Marketing Application")
    else:
        st.session_state.initialized = True
        
if st.session_state.get('need_rerun', False):
    st.session_state.need_rerun = False
    st.rerun()