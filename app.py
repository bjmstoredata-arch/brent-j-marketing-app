# app.py
import streamlit as st
import pandas as pd
from db_utils import load_data, create_tables, migrate_schema
from logic import (
    add_new_client, add_vin_to_client, add_part_to_vin,
    add_part_without_vin, delete_client, delete_vin,
    delete_part, update_client_and_vins, update_part,
    add_supplier_to_part, safe_add_part_to_vin
)
from security import validate_phone, validate_vin, validate_numeric
from data_utils import safe_get_value, safe_get_first_row
from fpdf import FPDF
import random
from datetime import datetime
import base64

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
# ---

# --- Ensure tables are created when the app first runs ---
create_tables()
migrate_schema()

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

def reset_session_state():
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
    st.cache_data.clear()
    st.rerun()

def main_navigation():
    st.sidebar.title("Navigation")
    if st.sidebar.button("🏠 Main Dashboard"):
        reset_session_state()
    if st.sidebar.button("👥 Clients"):
        st.session_state.view = 'client_list'
        st.rerun()
    if st.sidebar.button("📦 Parts Inventory"):
        st.session_state.view = 'view_parts_inventory'
        st.rerun()
    if st.sidebar.button("📄 Generate Quote"):
        st.session_state.view = 'generate_pdf_flow'
        st.session_state.document_type = 'quote'
        st.rerun()
    if st.sidebar.button("🧾 Generate Invoice"):
        st.session_state.view = 'generate_pdf_flow'
        st.session_state.document_type = 'invoice'
        st.rerun()
    if st.sidebar.button("📝 Text Quote"):
        st.session_state.view = 'generate_text_quote_flow'
        st.rerun()
    
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
                st.rerun()

def export_data():
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Export")
    
    if st.sidebar.button("Export All Data to CSV"):
        import io
        import zipfile
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            for name, df in [("clients", df_clients), ("vins", df_vins), 
                            ("parts", df_parts), ("part_suppliers", df_part_suppliers)]:
                csv_data = df.to_csv(index=False)
                zip_file.writestr(f"{name}.csv", csv_data)
        
        zip_buffer.seek(0)
        st.sidebar.download_button(
            label="Download ZIP",
            data=zip_buffer,
            file_name="brent_j_marketing_data_export.zip",
            mime="application/zip"
        )

def backup_database():
    import shutil
    import datetime
    
    if st.sidebar.button("Backup Database"):
        backup_name = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2('brent_j_marketing.db', backup_name)
        st.sidebar.success(f"Backup created: {backup_name}")

def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.write("Please try again or contact support if the problem persists.")
            return None
    return wrapper

def status_indicator(message, status="info"):
    if status == "success":
        st.success(f"✅ {message}")
    elif status == "warning":
        st.warning(f"⚠️ {message}")
    elif status == "error":
        st.error(f"❌ {message}")
    else:
        st.info(f"ℹ️ {message}")

# --- UI LOGIC ---
st.title("Brent J. Marketing, car parts database")
df_clients, df_vins, df_parts, df_part_suppliers = load_data()

# Add navigation sidebar
main_navigation()
global_search()
export_data()
backup_database()

# --- SEARCH AND MAIN SCREEN ---
if st.session_state.view == 'main':
    st.header("Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("➕ Add Client", help="Add a new client"):
            st.session_state.view = 'add_client'
            st.rerun()
    with col2:
        if st.button("🔍 Find Client", help="Search for a client"):
            st.session_state.view = 'client_list'
            st.rerun()
    with col3:
        if st.button("📦 Add Part", help="Add a new part"):
            st.session_state.view = 'add_part_without_vin_flow'
            st.rerun()
    
    st.header("Search for Client")
    search_term = st.text_input("Search by exact Phone Number", key="search_clients")

    if search_term:
        with st.spinner("Searching for client..."):
            found_client = df_clients[df_clients['phone'].astype(str) == search_term]
            if not found_client.empty:
                st.session_state.view = 'client_details'
                st.session_state.current_client_phone = found_client['phone'].iloc[0]
                st.session_state.current_client_name = found_client['client_name'].iloc[0]
                st.rerun()
            else:
                st.warning("No client found with that exact phone number.")
                if st.button("Register New Client"):
                    st.session_state.view = 'add_client'
                    st.session_state.show_client_form = True
                    st.rerun()
    
    st.divider()

    col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])
    with col1:
        if st.button("Add New Client"):
            st.session_state.show_client_form = True
            st.session_state.view = 'add_client'
            st.rerun()
    with col2:
        if st.button("View All Clients"):
            st.session_state.view = 'client_list'
            st.rerun()
    with col3:
        if st.button("View Parts"):
            st.session_state.view = 'view_parts_inventory'
            st.rerun()
    with col4:
        if st.button("Generate Quote (PDF)"):
            st.session_state.view = 'generate_pdf_flow'
            st.session_state.document_type = 'quote'
            st.rerun()
    with col5:
        if st.button("Generate Invoice (PDF)"):
            st.session_state.view = 'generate_pdf_flow'
            st.session_state.document_type = 'invoice'
            st.rerun()
    with col6:
        if st.button("Generate Quote (Text)"):
            st.session_state.view = 'generate_text_quote_flow'
            st.rerun()

# --- Generate Quote/Invoice Flow ---
elif st.session_state.view == 'generate_pdf_flow':
    doc_type = st.session_state.document_type
    st.header(f"Generate {doc_type.capitalize()} (PDF)")
    if st.button("⬅️ Back to Main"):
        reset_session_state()
    
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
            
            selected_part_ids = []
            st.markdown("---")
            st.markdown("### Select Parts to Include in Document:")
            
            for _, part_row in parts_to_display.iterrows():
                # Get the selling price for the part
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                selling_price = part_suppliers['selling_price'].iloc[0] if not part_suppliers.empty else 0.0

                part_label = f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']} - Price: ${selling_price:.2f}"
                if pd.notna(part_row['vin_number']):
                    part_label += f" (VIN: {part_row['vin_number']})"
                
                if st.checkbox(part_label, key=f"pdf_checkbox_{part_row['id']}"):
                    selected_part_ids.append(part_row['id'])

            st.markdown("---")
            
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

            # Step 4: Generate the quote
            if st.button(f"Generate {doc_type.capitalize()} (PDF)"):
                if not selected_part_ids:
                    st.warning(f"Please select at least one part to generate a {doc_type}.")
                else:
                    # Generate the document number
                    if doc_type == 'quote':
                        document_number = f"BJM-{random.randint(1000, 9999)}-Q"
                    else:
                        document_number = f"BJM-{random.randint(1000, 9999)}-I"

                    # Find a delivery time from the selected parts
                    delivery_time = "IN STOCK"  # Default value
                    for part_id in selected_part_ids:
                        part_suppliers_rows = df_part_suppliers[df_part_suppliers['part_id'] == part_id]
                        if not part_suppliers_rows.empty and pd.notna(part_suppliers_rows['delivery_time'].iloc[0]):
                            delivery_time_value = part_suppliers_rows['delivery_time'].iloc[0]
                            if delivery_time_value and delivery_time_value.strip():  # Check if not empty
                                delivery_time = delivery_time_value
                                break

                    # Gather data for the PDF
                    client_info = {
                        'name': selected_client_name,
                        'phone': st.session_state.quote_selected_phone,
                        'vin_number': st.session_state.quote_selected_vin,
                    }
                    parts_data = []
                    total_quote_amount = 0
                    for part_id in selected_part_ids:
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        part_suppliers_rows = df_part_suppliers[df_part_suppliers['part_id'] == part_id]
                        selling_price = part_suppliers_rows['selling_price'].iloc[0] if not part_suppliers_rows.empty else 0.0
                        
                        total_quote_amount += (part_row['quantity'] * selling_price)
                        
                        parts_data.append({
                            'name': part_row['part_name'],
                            'quantity': part_row['quantity'],
                            'price': selling_price,
                        })
                    
                    # Package the custom info
                    bill_to_info = {"name": bill_to_name, "address": bill_to_address} if customize_bill else None
                    ship_to_info = {"name": ship_to_name, "address": ship_to_address} if customize_ship else None

                    # Generate PDF
                    pdf_bytes = bytes(generate_pdf(client_info, parts_data, total_quote_amount, manual_deposit, bill_to_info, ship_to_info, delivery_time, document_number, doc_type))
                    
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
                    st.rerun()

        else:
            st.info(f"No parts found for this client/VIN. Please add parts first.")

# --- Generate Text Quote Flow ---
elif st.session_state.view == 'generate_text_quote_flow':
    st.header("Generate Quote (Text)")
    if st.button("⬅️ Back to Main"):
        reset_session_state()
    
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
            
            selected_part_ids = []
            st.markdown("---")
            st.markdown("### Select Parts to Include in Quote:")
            
            for _, part_row in parts_to_display.iterrows():
                # Get the selling price for the part
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                selling_price = part_suppliers['selling_price'].iloc[0] if not part_suppliers.empty else 0.0

                part_label = f"**{part_row['part_name']}** ({part_row['part_number']}) - Qty: {part_row['quantity']} - Price: ${selling_price:.2f}"
                if pd.notna(part_row['vin_number']):
                    part_label += f" (VIN: {part_row['vin_number']})"
                
                if st.checkbox(part_label, key=f"quote_text_checkbox_{part_row['id']}"):
                    selected_part_ids.append(part_row['id'])

            st.markdown("---")
            
            # Add option to specify if parts are used or new
            if selected_part_ids:
                st.subheader("Part Condition")
                part_conditions = {}
                
                # Create a container for part conditions to avoid overwriting
                condition_container = st.container()
                
                with condition_container:
                    for part_id in selected_part_ids:
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        condition = st.selectbox(
                            f"Condition for {part_row['part_name']}",
                            ["New", "Used", "Refurbished"],
                            key=f"condition_{part_id}"  # Unique key for each selectbox
                        )
                        part_conditions[part_id] = condition
            
            if st.button("Generate Text Quote"):
                if not selected_part_ids:
                    st.warning("Please select at least one part to generate a text quote.")
                else:
                    # Find a delivery time from the selected parts
                    delivery_time = "IN STOCK"  # Default value
                    for part_id in selected_part_ids:
                        part_suppliers_rows = df_part_suppliers[df_part_suppliers['part_id'] == part_id]
                        if not part_suppliers_rows.empty and pd.notna(part_suppliers_rows['delivery_time'].iloc[0]):
                            delivery_time_value = part_suppliers_rows['delivery_time'].iloc[0]
                            if delivery_time_value and delivery_time_value.strip():  # Check if not empty
                                delivery_time = delivery_time_value
                                break
                    
                    # Build the text quote string with the new format
                    quote_text = "*SEE PRICES BELOW*\n\n"
                    
                    # Fix for IN STOCK display
                    if delivery_time == "IN STOCK":
                        quote_text += "*IN STOCK*\n\n"
                    else:
                        quote_text += f"*DELIVERY WITHIN {delivery_time} BUSINESS DAYS*\n\n"
                        
                    quote_text += "*CASH (At Our Office) OR ONLINE BANK TRANSFER ONLY*\n\n"
                    quote_text += "*Upon Confirmation An Official Quote Will Be Sent With Payment Details.*\n\n"
                    
                    if st.session_state.quote_selected_vin and st.session_state.quote_selected_vin != 'Show All Parts':
                        quote_text += f"*Vin #* {st.session_state.quote_selected_vin}\n\n"
                    
                    part_counter = 1
                    for part_id in selected_part_ids:
                        part_row = df_parts[df_parts['id'] == part_id].iloc[0]
                        part_suppliers_rows = df_part_suppliers[df_part_suppliers['part_id'] == part_id]
                        selling_price = part_suppliers_rows['selling_price'].iloc[0] if not part_suppliers_rows.empty else 0.0
                        
                        # Get the condition for this part
                        condition = part_conditions.get(part_id, "New")
                        
                        quote_text += f"{part_counter}) {part_row['part_name']} - {part_row['quantity']} - ${selling_price:.2f} ({condition.lower()} item)\n"
                        part_counter += 1
                    
                    st.session_state.generated_text_quote = quote_text
            
            if st.session_state.generated_text_quote:
                st.markdown("---")
                st.subheader("Copy and Paste Quote")
                st.code(st.session_state.generated_text_quote)
                
                # Add a copy button
                if st.button("Copy to Clipboard"):
                    st.write("Quote copied to clipboard!")
                
                st.button("Clear Quote", on_click=lambda: st.session_state.update(generated_text_quote=""))

        else:
            st.info("No parts found for this client/VIN. Please add parts first.")

# --- Parts Inventory View ---
elif st.session_state.view == 'view_parts_inventory':
    st.header("View Parts")
    if st.button("⬅️ Back to Main"):
        reset_session_state()
    
    st.markdown("---")
    
    search_query = st.text_input("🔍 Search parts by name or number")
    
    # Filter the parts data based on the search query
    filtered_parts = df_parts
    if search_query:
        filtered_parts = df_parts[
            df_parts['part_name'].str.contains(search_query, case=False, na=False) |
            df_parts['part_number'].str.contains(search_query, case=False, na=False)
        ]

    if not filtered_parts.empty:
        st.write(f"Displaying **{len(filtered_parts)}** results.")
        
        with st.expander("View All Parts", expanded=False):
            st.dataframe(filtered_parts[['part_name', 'part_number', 'quantity', 'date_added', 'client_phone', 'vin_number']], use_container_width=True, hide_index=True)

        for _, part_row in filtered_parts.iterrows():
            with st.expander(f"Details for {part_row['part_name']} ({part_row['part_number']})"):
                st.subheader("Part Information")
                st.write(f"**Part ID:** {part_row['id']}")
                st.write(f"**Quantity:** {part_row['quantity']}")
                st.write(f"**Notes:** {part_row['notes']}")
                st.write(f"**Client Phone:** {part_row['client_phone']}")
                st.write(f"**VIN:** {part_row['vin_number']}")
                
                st.markdown("---")
                st.subheader("Supplier Information")
                
                part_suppliers = df_part_suppliers[df_part_suppliers['part_id'] == part_row['id']]
                
                if not part_suppliers.empty:
                    st.dataframe(part_suppliers[['supplier_name', 'buying_price', 'selling_price', 'delivery_time']], use_container_width=True, hide_index=True)
                else:
                    st.info("No suppliers found for this part.")

    else:
        st.info("No parts found matching your search criteria.")

# --- CLIENT LIST VIEW ---
elif st.session_state.view == 'client_list':
    st.header("All Clients")
    if st.button("⬅️ Back to Main"):
        reset_session_state()

    st.divider()

    if not df_clients.empty:
        col_header1, col_header2, col_header3 = st.columns([0.4, 0.3, 0.3])
        with col_header1:
            st.markdown("### Client Name")
        with col_header2:
            st.markdown("### Phone")
        with col_header3:
            st.markdown("### Actions")
        
        st.markdown("---")

        for index, client_row in df_clients.iterrows():
            col1, col2, col3 = st.columns([0.4, 0.3, 0.3])
            with col1:
                st.write(client_row['client_name'])
            with col2:
                st.write(client_row['phone'])
            with col3:
                action_col1, action_col2 = st.columns([1, 1])
                with action_col1:
                    if st.button("View Details", key=f"view_details_{client_row['phone']}"):
                        st.session_state.view = 'client_details'
                        st.session_state.current_client_phone = client_row['phone']
                        st.session_state.current_client_name = client_row['client_name']
                        st.rerun()
                with action_col2:
                    delete_client_key = f"delete_client_{client_row['phone']}"
                    if st.button("❌", help="Delete Client", key=delete_client_key):
                        if st.session_state.get(f'confirm_delete_client_{client_row["phone"]}', False):
                            delete_client(client_row['phone'])
                            st.success(f"Client `{client_row['client_name']}` and all associated data deleted.")
                            st.session_state.pop(f'confirm_delete_client_{client_row["phone"]}', None)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.session_state[f'confirm_delete_client_{client_row["phone"]}'] = True
                            st.warning("Are you sure? Click again to confirm.")
                    if st.session_state.get(f'confirm_delete_client_{client_row["phone"]}', False) and st.button("Cancel", key=f'cancel_delete_client_{client_row["phone"]}'):
                        st.session_state.pop(f'confirm_delete_client_{client_row["phone"]}', None)
                        st.rerun()
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
                reset_session_state()
        with col2:
            if st.button("Edit Client"):
                st.session_state.edit_mode = True
                st.rerun()
        with col3:
            if st.button("Add VIN"):
                st.session_state.view = 'add_vin_to_existing_client'
                st.rerun()
        with col4:
            if st.button("Add Part to Existing VIN"):
                st.session_state.selected_vin_to_add_part = None
                st.session_state.view = 'add_part_for_client'
                st.rerun()
        with col5:
            if st.button("Add Part Without VIN"):
                st.session_state.view = 'add_part_without_vin_for_client'
                st.rerun()

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
                            delete_part(part_row['id'])
                            st.cache_data.clear()
                            st.rerun()
                    with col_part4:
                        if st.button("✏️", key=f"edit_part_unassigned_{part_row['id']}"):
                            st.session_state.view = 'edit_part'
                            st.session_state.part_to_edit_id = part_row['id']
                            st.rerun()

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
                                delete_vin(vin_row['vin_number'])
                                st.success(f"VIN `{vin_row['vin_number']}` and all associated parts deleted.")
                                st.session_state.pop(f'confirm_delete_vin_{vin_row["vin_number"]}', None)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.session_state[f'confirm_delete_vin_{vin_row["vin_number"]}'] = True
                                st.warning("Are you sure? Click again to confirm.")
                        if st.session_state.get(f'confirm_delete_vin_{vin_row["vin_number"]}', False) and st.button("Cancel", key=f'cancel_delete_{vin_row["vin_number"]}'):
                            st.session_state.pop(f'confirm_delete_vin_{vin_row["vin_number"]}', None)
                            st.rerun()

                        if st.button(f"Add Part", key=f'add_part_vin_{vin_row["vin_number"]}'):
                            st.session_state.selected_vin_to_add_part = vin_row['vin_number']
                            st.session_state.view = 'add_part_for_client'
                            st.rerun()

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
                                        delete_part(part_row['id'])
                                        st.cache_data.clear()
                                        st.rerun()
                                if st.button("✏️", key=f"edit_part_{part_row['id']}"):
                                    st.session_state.view = 'edit_part'
                                    st.session_state.part_to_edit_id = part_row['id']
                                    st.rerun()
                                
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
                    update_client_and_vins(old_phone, new_phone, new_name)
                    st.success("✅ Client and associated VINs updated successfully!")
                    st.session_state.edit_mode = False
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.edit_mode = False
                    st.rerun()
            with col3:
                if st.form_submit_button("View VINs"):
                    st.session_state.edit_mode = False
                    st.rerun()

# --- NEW: EDIT PART VIEW ---
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
                update_part(part_id, part_name, part_number, quantity, notes, suppliers_data)
                st.success("✅ Part updated successfully!")
                st.session_state.view = 'client_details'
                st.session_state.part_to_edit_id = None
                st.session_state.supplier_count_edit = 1
                st.cache_data.clear()
                st.rerun()
        with col2:
            if st.form_submit_button("Cancel"):
                st.session_state.view = 'client_details'
                st.session_state.part_to_edit_id = None
                st.session_state.supplier_count_edit = 1
                st.rerun()

# --- VIN DETAILS VIEW ---
elif st.session_state.view == 'vin_details':
    st.header(f"VIN Details: {st.session_state.current_vin_no_view}")
    
    if st.button("Back to Client Details"):
        st.session_state.view = 'client_details'
        st.rerun()
    
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
        tab1, tab2 = st.tabs(["Part Details", "Supplier Details"])

        with tab1:
            st.button("Add another part", on_click=lambda: st.session_state.update(part_count=st.session_state.part_count + 1), key="add_another_part_button")
            st.markdown("---")
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
                    submitted = st.form_submit_button("Save Part(s) and Continue to Suppliers")
                with col2:
                    if st.form_submit_button("Cancel"):
                        reset_session_state()

                if submitted:
                    all_valid = True
                    validation_errors = []
                    for idx, part in enumerate(parts_data):
                        if not part["name"] and not part["number"]:
                            all_valid = False
                            validation_errors.append(f"Part {idx+1}: Name or Number is required")
                    
                    if all_valid:
                        part_ids = []
                        with st.spinner("Saving parts..."):
                            for part in parts_data:
                                try:
                                    if st.session_state.view in ['add_part_to_existing_vin', 'add_part_for_client']:
                                        # Safely get client phone from VIN
                                        vin_match = df_vins[df_vins['vin_number'] == st.session_state.selected_vin_to_add_part]
                                        if vin_match.empty:
                                            st.error(f"VIN {st.session_state.selected_vin_to_add_part} not found in database")
                                            return
                    
                                        client_phone = vin_match['client_phone'].iloc[0]
                                        part_id = safe_add_part_to_vin(
                                            st.session_state.selected_vin_to_add_part,
                                            client_phone,
                                            part,
                                            [] # No suppliers added at this stage
                                        )
                                    elif st.session_state.view == 'add_part_without_vin_flow':
                                        part_id = add_part_without_vin(
                                            part["name"],
                                            part["number"],
                                            part["quantity"],
                                            part["notes"],
                                            client_phone=None,
                                            suppliers=[]
                                        )
                                    elif st.session_state.view == 'add_part_without_vin_for_client':
                                        part_id = add_part_without_vin(
                                            part["name"],
                                            part["number"],
                                            part["quantity"],
                                            part["notes"],
                                            client_phone=st.session_state.current_client_phone,
                                            suppliers=[]
                                        )
                
                                    if part_id:
                                        part_ids.append(part_id)
                                    else:
                                        st.error(f"Failed to save part: {part.get('name', 'Unknown')}")
                    
                                except Exception as e:
                                    st.error(f"Error saving part '{part.get('name', 'Unknown')}': {str(e)}")
                                    # Continue with other parts instead of failing completely
                                    continue
    
                        if part_ids:
                            st.session_state.current_part_id_to_add_supplier = part_ids[0]
                            st.session_state.last_part_ids = part_ids
                            st.success(f"✅ {len(part_ids)} part(s) saved! Please add supplier details in the next tab.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("No parts were saved successfully")
                    else:
                        for error in validation_errors:
                            st.warning(error)

        with tab2:
            st.header("Add Supplier Details")
            if st.session_state.current_part_id_to_add_supplier:
                # Find the part information from the DataFrame
                part_info = df_parts[df_parts['id'] == st.session_state.current_part_id_to_add_supplier].iloc[0]
                part_name = part_info['part_name']
                part_number = part_info['part_number']

                # Update the info message to include the part name and number
                st.info(f"Adding suppliers for: **{part_name}** (Part Number: **{part_number}**)")
                
                def add_supplier_field_add():
                    st.session_state.supplier_count += 1
                
                def remove_supplier_field():
                    if st.session_state.supplier_count > 1:
                        st.session_state.supplier_count -= 1

                col1, col2 = st.columns([1,1])
                with col1:
                    st.button("Add another supplier", on_click=add_supplier_field_add, key="add_supplier_button")
                with col2:
                    if st.session_state.supplier_count > 1:
                        st.button("Remove last supplier", on_click=remove_supplier_field, key="remove_supplier_button")

                with st.form("add_supplier_form", clear_on_submit=True):
                    suppliers_data = []
                    for i in range(st.session_state.supplier_count):
                        st.subheader(f"Supplier {i+1}")
                        supplier_name = st.text_input("Supplier Name*", key=f"supplier_name_{i}", help="Required field")
                        buying_price = st.number_input("Buying Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"buying_price_{i}")
                        selling_price = st.number_input("Selling Price ($)", min_value=0.0, value=0.0, format="%.2f", key=f"selling_price_{i}")
                        delivery_time = st.text_input("Delivery Time", key=f"delivery_time_{i}")
                        
                        suppliers_data.append({
                            "name": supplier_name,
                            "buying_price": buying_price,
                            "selling_price": selling_price,
                            "delivery_time": delivery_time
                        })
                    
                    submitted_suppliers = st.form_submit_button("Save Suppliers")
                    if submitted_suppliers:
                        validation_errors = []
                        for idx, supplier in enumerate(suppliers_data):
                            if not supplier['name']:
                                validation_errors.append(f"Supplier {idx+1}: Name is required")
                        
                        if not validation_errors:
                            with st.spinner("Saving suppliers..."):
                                for supplier in suppliers_data:
                                    try:
                                        add_supplier_to_part(
                                            st.session_state.current_part_id_to_add_supplier,
                                            supplier['name'],
                                            supplier['buying_price'],
                                            supplier['selling_price'],
                                            supplier['delivery_time']
                                        )
                                    except Exception as e:
                                        st.error(f"Error saving supplier: {str(e)}")
                                        return
                                
                            st.success("✅ All suppliers saved!")
                            reset_part_entry()
                            st.cache_data.clear()
                            
                            if st.session_state.view in ['add_part_without_vin_flow', 'add_part_to_existing_vin']:
                                st.session_state.view = 'main'
                            else:
                                st.session_state.view = 'client_details'
                            st.rerun()
                        else:
                            for error in validation_errors:
                                st.warning(error)
            else:
                st.info("Please add and save a part first in the 'Part Details' tab.")
            
    # --- Call render_part_forms() for the 'add_part' views ---
    if st.session_state.view in ['add_part_to_existing_vin', 'add_part_without_vin_flow', 'add_part_without_vin_for_client', 'add_part_for_client']:
        if st.session_state.view == 'add_part_to_existing_vin':
            st.header(f"Add Part to VIN: {st.session_state.selected_vin_to_add_part}")
        elif st.session_state.view == 'add_part_without_vin_flow':
            st.header("Add Part Without VIN")
        elif st.session_state.view == 'add_part_for_client':
            st.header(f"Add Part for Client: {st.session_state.current_client_name}")
            st.write(f"VIN selected: {st.session_state.selected_vin_to_add_part}")
        elif st.session_state.view == 'add_part_without_vin_for_client':
            st.header(f"Add Part for Client: {st.session_state.current_client_name} (No VIN)")

        if st.button("⬅️ Back"):
            if st.session_state.view in ['add_part_without_vin_flow', 'add_part_to_existing_vin']:
                st.session_state.view = 'main'
            else:
                st.session_state.view = 'client_details'
            st.rerun()

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
                    st.rerun()
            
            if submitted_vin:
                if vin_no and not validate_vin(vin_no):
                    st.error("Please enter a valid VIN (13-17 alphanumeric characters) or leave blank")
                else:
                    st.session_state.vin_added = True
                    st.session_state.current_vin_no = vin_no
                    st.rerun()

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
                    st.rerun()
            
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
                    
                    add_vin_to_client(
                        str(st.session_state.current_client_phone),
                        clean_vin,
                        model,
                        prod_yr,
                        body,
                        engine,
                        code,
                        transmission
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
                    st.rerun()
                    
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
                    reset_session_state()
            
            if submitted:
                if phone:
                    if not validate_phone(phone):
                        st.error("Please enter a valid phone number (7-15 digits)")
                    else:
                        try:
                            add_new_client(str(phone), client_name)
                            st.session_state.client_added = True
                            st.session_state.current_client_phone = str(phone)
                            st.session_state.current_client_name = client_name
                            st.cache_data.clear()
                            st.rerun()
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
                    st.rerun()
            
            if submitted_vin:
                if vin_no and not validate_vin(vin_no):
                    st.error("Please enter a valid VIN (13-17 alphanumeric characters) or leave blank")
                else:
                    st.session_state.vin_added = True
                    st.session_state.current_vin_no = vin_no
                    st.rerun()
    
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
                    st.rerun()
            
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
                    
                    add_vin_to_client(
                        str(st.session_state.current_client_phone),
                        clean_vin,
                        model,
                        prod_yr,
                        body,
                        engine,
                        code,
                        transmission
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
                    st.rerun()
                    
                except ValueError as e:
                    st.error(str(e))

# --- SEARCH RESULTS VIEW ---
elif st.session_state.view == 'search_results':
    st.header("Search Results")
    if st.button("⬅️ Back to Main"):
        reset_session_state()
    
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