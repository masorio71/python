import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import numpy as np
import toml
import os
import time
import msal
import requests

import plotly.express as px

# Configurazione Pagina
st.set_page_config(
    page_title="Dashboard Eventi",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inizializzazione Client Supabase
@st.cache_resource
def init_supabase(url, key):
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"Errore durante l'inizializzazione di Supabase: {e}")
        return None

# Variabili globali per le credenziali e il client Supabase
supabase_url_global = st.secrets.get("supabase", {}).get("url", "")
supabase_key_global = st.secrets.get("supabase", {}).get("key", "")
DB_TABLE_NAME = st.secrets.get("supabase", {}).get("table_name", "eventi_importati")
supabase = init_supabase(supabase_url_global, supabase_key_global)

# --- CONFIGURATION HELPERS ---
def get_config(key):
    """Fetch a configuration value from the app_config table."""
    if not supabase: return ""
    try:
        response = supabase.table("app_config").select("value").eq("key", key).execute()
        if response.data and len(response.data) > 0:
            return response.data[0]['value']
        return ""
    except Exception as e:
        # Fail silently or log if table doesn't exist yet
        return ""

def update_config(key, value):
    """Upsert a configuration value into the app_config table."""
    if not supabase: return False
    try:
        supabase.table("app_config").upsert({"key": key, "value": value}).execute()
        return True
    except Exception as e:
        st.error(f"Errore salvataggio config ({key}): {e}")
        return False

def get_user_role(email):
    """
    Fetch user role from authorized_users table.
    Returns 'Amministratore' or 'Visitatore'.
    """
    if not email or not supabase:
        return None
    try:
        response = supabase.table("authorized_users").select("role").eq("email", email).execute()
        if response.data and len(response.data) > 0:
            return response.data[0].get('role', 'Visitatore')
        return None
    except Exception as e:
        return None

# Funzione per la pagina di Configurazione
def render_config_page():
    st.title("⚙️ Configurazione")
    
    # --- 1. CONFIGURAZIONE LOCALE (st.secrets) ---
    st.info("Le impostazioni qui sotto sono salvate nel file locale `secrets.toml`. Servono per la connessione al DB e servizi base.")
    
    # Load current values from secrets (safe access)
    current_supabase_url = st.secrets.get("supabase", {}).get("url", "")
    current_supabase_key = st.secrets.get("supabase", {}).get("key", "")
    current_table_name = st.secrets.get("supabase", {}).get("table_name", "eventi_importati")
    current_tmdb_key = st.secrets.get("tmdb", {}).get("api_key", "")
    
    with st.form("config_form"):
        st.subheader("Credenziali Supabase")
        url_input = st.text_input("Supabase URL", value=current_supabase_url, type="password")
        key_input = st.text_input("Supabase Key", value=current_supabase_key, type="password")
        table_name_input = st.text_input("Nome Tabella Supabase", value=current_table_name)
        
        st.subheader("Credenziali TMDB")
        tmdb_input = st.text_input("TMDB API Key", value=current_tmdb_key, type="password")

        st.subheader("Credenziali Microsoft 365")
        ms_tenant = st.secrets.get("microsoft", {}).get("tenant_id", "")
        ms_client_id = st.secrets.get("microsoft", {}).get("client_id", "")
        ms_client_secret = st.secrets.get("microsoft", {}).get("client_secret", "")
        
        tenant_input = st.text_input("Tenant ID", value=ms_tenant, type="password")
        client_id_input = st.text_input("Client ID", value=ms_client_id, type="password")
        client_secret_input = st.text_input("Client Secret", value=ms_client_secret, type="password")
        
        submitted = st.form_submit_button("Salva Configurazione Locale")
        
        if submitted:
            if not url_input or not key_input or not tmdb_input:
                st.error("Tutti i campi sono obbligatori.")
            else:
                try:
                    # Prepare data structure
                    new_secrets = {
                        "supabase": {
                            "url": url_input,
                            "key": key_input,
                            "table_name": table_name_input
                        },
                        "tmdb": {
                            "api_key": tmdb_input
                        },
                        "microsoft": {
                            "tenant_id": tenant_input,
                            "client_id": client_id_input,
                            "client_secret": client_secret_input
                        }
                    }
                    
                    # Write to secrets.toml
                    secrets_path = ".streamlit/secrets.toml"
                    os.makedirs(".streamlit", exist_ok=True)
                    
                    # Load existing to preserve other secrets if any
                    current_secrets = {}
                    if os.path.exists(secrets_path):
                        try:
                            current_secrets = toml.load(secrets_path)
                        except Exception:
                            pass
                            
                    current_secrets["supabase"] = new_secrets["supabase"]
                    current_secrets["tmdb"] = new_secrets["tmdb"]
                    current_secrets["microsoft"] = new_secrets["microsoft"]
                    
                    with open(secrets_path, "w") as f:
                        toml.dump(current_secrets, f)
                        
                    # Update global variables immediately
                    global supabase_url_global, supabase_key_global, supabase, DB_TABLE_NAME
                    supabase_url_global = url_input
                    supabase_key_global = key_input
                    DB_TABLE_NAME = table_name_input
                    supabase = init_supabase(supabase_url_global, supabase_key_global)
                    
                    st.success("Configurazione locale salvata con successo! L'applicazione si aggiornerà.")
                    
                except Exception as e:
                    st.error(f"Errore durante il salvataggio: {e}")
    
    # --- 2. INTEGRAZIONI DATABASE (app_config) ---
    st.divider()
    st.subheader("🔌 Integrazione Brevo (SMS)")
    st.markdown("Queste impostazioni sono salvate nel **database** e servono per l'invio di SMS.")

    # Check connection
    if not supabase:
        st.error("Connettiti a Supabase prima di configurare Brevo.")
    else:
        current_brevo_key = get_config("brevo_api_key")
        current_sms_sender = get_config("brevo_sms_sender")
        current_sms_list_id = get_config("brevo_sms_list_id")
        current_sms_template = get_config("brevo_sms_template_content")
        
        # Row 1: API Key
        new_brevo_key = st.text_input("Brevo API Key", value=current_brevo_key, type="password", key="brevo_key_in")
        
        # Row 2: Sender + List ID
        col_sender, col_list = st.columns([3, 1])
        with col_sender:
             new_sms_sender = st.text_input("Nome Mittente (max 11 car.)", value=current_sms_sender, max_chars=11, help="Nome che apparirà come mittente (alfanumerico).", key="brevo_sender_in")
        with col_list:
             new_sms_list_id = st.text_input("ID Lista Contatti", value=current_sms_list_id, help="ID numerico lista Brevo", key="brevo_list_in")
             
        # Row 3: Template Content
        new_sms_template = st.text_area("Messaggio Standard (Template)", value=current_sms_template, height=100, max_chars=160, help="Il testo che verrà inviato alla lista. Max 160 caratteri.", key="brevo_tmpl_in")

        if st.button("Salva Configurazione SMS", type="primary"):
            success = True
            if not update_config("brevo_api_key", new_brevo_key): success = False
            if not update_config("brevo_sms_sender", new_sms_sender): success = False
            if not update_config("brevo_sms_list_id", new_sms_list_id): success = False
            if not update_config("brevo_sms_template_content", new_sms_template): success = False
            
            if success:
                st.success("Configurazione SMS aggiornata correttamente nel database!")
            else:
                st.error("Errore durante il salvataggio nel database.")

        # --- TEST AREA SMS ---
        with st.expander("🧪 Area di Test SMS", expanded=False):
            st.info("Invia un SMS di prova usando la configurazione ATTUALE (Database).")
            
            # Pre-fill with template content if available
            default_msg = current_sms_template if current_sms_template else ""
            
            test_phone = st.text_input("Numero Destinatario", value="+39")
            test_message = st.text_area("Messaggio del Test", value=default_msg, max_chars=160, height=100)
            
            if st.button("Invia SMS di Test"):
                # Fetch fresh config to ensure we use what's in DB
                api_key_test = get_config("brevo_api_key")
                sender_test = get_config("brevo_sms_sender")
                
                if not api_key_test:
                    st.error("API Key mancante.")
                elif not sender_test:
                    st.error("Mittente mancante.")
                elif not test_phone or len(test_phone) < 5:
                    st.error("Numero di telefono non valido.")
                elif not test_message:
                    st.error("Messaggio vuoto.")
                else:
                    try:
                        # Brevo SMS API Payload
                        url = "https://api.brevo.com/v3/transactionalSMS/sms"
                        payload = {
                            "sender": sender_test,
                            "recipient": test_phone,
                            "content": test_message
                        }
                        headers = {
                            "api-key": api_key_test,
                            "Content-Type": "application/json",
                            "accept": "application/json"
                        }
                        
                        with st.spinner("Invio SMS in corso..."):
                            response = requests.post(url, json=payload, headers=headers)
                        
                        if response.status_code in [200, 201]:
                            st.success("SMS inviato con successo!")
                            st.json(response.json())
                        else:
                            st.error(f"Errore Brevo ({response.status_code}): {response.text}")
                    except Exception as e:
                        st.error(f"Eccezione durante l'invio SMS: {e}")

# Funzione per la pagina di Importazione (Logica esistente)
def render_import_page():
    st.title("📥 Importa Dati")
    
    global supabase_url_global, supabase_key_global, supabase

    # Check credentials and TMDB key
    tmdb_key_check = st.secrets.get("tmdb", {}).get("api_key")
    
    if not supabase_url_global or not supabase_key_global or supabase is None or not tmdb_key_check:
        st.warning("Configurazione mancante. Vai alla pagina 'Configurazione' nel menu laterale per inserire le chiavi API.")
        return
    
    st.success("Credenziali caricate correttamente.")

    # Helper function for Event Logic (Used by both imports)
    def get_evento(dt):
        if pd.isna(dt):
            return None
        if dt.weekday() == 4:
            return "ven"
        current_time = dt.time()
        start_time = pd.Timestamp("14:00:00").time()
        end_time = pd.Timestamp("16:00:00").time()
        if start_time <= current_time <= end_time:
            return "bam"
        return "adu"

    # --- SECTION 1: ExportEventi.xlsx ---
    st.subheader("Carica file ExportEventi.xlsx")
    uploaded_file = st.file_uploader("Scegli un file Excel", type=['xlsx', 'xls'])

    if uploaded_file is not None:
        try:
            try:
                df_uploaded = pd.read_excel(uploaded_file, sheet_name='export_titolieventi', header=1)
            except ValueError:
                st.error("Il foglio 'export_titolieventi' non è stato trovato nel file Excel.")
                st.stop()

            if df_uploaded.shape[1] < 12:
                 st.error("Il file Excel non ha abbastanza colonne (richieste almeno fino alla colonna L).")
                 st.stop()

            # Updated Indices: A(0), B(1), C(2), D(3), E(4), F(5), I(8), L(11)
            col_indices = [0, 1, 2, 3, 4, 5, 8, 11]
            selected_columns = df_uploaded.iloc[:, col_indices].copy()
            
            # Strict Renaming
            selected_columns.columns = [
                'data_inizio', 'data_fine', 'Nr. Eventi', 'Titolo Evento', 'autore', 
                'Nazionalità', 'Tot. Presenze', 'Incasso'
            ]
            
            # Capture temp_datetime from data_inizio to process Event/Rassegna logic correctly
            selected_columns['temp_datetime'] = pd.to_datetime(selected_columns['data_inizio'], dayfirst=True, errors='coerce')

            # Date Cleaning (No Time)
            selected_columns['data_inizio'] = selected_columns['temp_datetime'].dt.strftime('%Y-%m-%d')
            selected_columns['data_fine'] = pd.to_datetime(selected_columns['data_fine'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
            

            
            # Autore Cleaning
            selected_columns['autore'] = selected_columns['autore'].astype(str).str.strip()
            
            # Numeric Cleaning
            selected_columns['Nr. Eventi'] = pd.to_numeric(selected_columns['Nr. Eventi'], errors='coerce').fillna(0).astype(int)
            selected_columns['Tot. Presenze'] = pd.to_numeric(selected_columns['Tot. Presenze'], errors='coerce').fillna(0).astype(int)
            selected_columns['Incasso'] = pd.to_numeric(selected_columns['Incasso'], errors='coerce').fillna(0.0)
            
            # Use shared get_evento
            selected_columns['Evento'] = selected_columns['temp_datetime'].apply(get_evento)
            
            # --- RASSEGNA Logic (New) ---
            def get_rassegna(row):
                dt = row['temp_datetime']
                if pd.isna(dt):
                    return "STANDARD"
                
                # 1. SCUOLE Rule: Time between 06:00 and 13:30
                current_time = dt.time()
                start_time = pd.Timestamp("06:00:00").time()
                end_time = pd.Timestamp("13:30:00").time()
                if start_time <= current_time <= end_time:
                    return "SCUOLE"
                
                # 2. CASTELLO Rule: Date between July 1st and September 15th
                md = (dt.month, dt.day)
                if (7, 1) <= md <= (9, 15):
                    return "CASTELLO"
                
                # 3. Fallback
                return "STANDARD"

            selected_columns['RASSEGNA'] = selected_columns.apply(get_rassegna, axis=1)

            event_name_col = 'Titolo Evento'
            
            def check_vos(val):
                if not isinstance(val, str):
                    return False
                val_lower = val.lower()
                keywords = ["(eng)", "(en)", "(originale)", "(vos)"]
                return any(k in val_lower for k in keywords)

            selected_columns['VOS'] = selected_columns[event_name_col].apply(check_vos)

            # Drop temp_datetime (Data is already set and formatted)
            selected_columns = selected_columns.drop(columns=['temp_datetime'])
            
            cols = ['data_inizio', 'data_fine', 'Evento', 'VOS', 'RASSEGNA'] + [c for c in selected_columns.columns if c not in ['data_inizio', 'data_fine', 'Evento', 'VOS', 'RASSEGNA']]
            df_uploaded = selected_columns[cols]

            st.success("File caricato ed elaborato con successo!")
            
            # --- 1. Row Exclusion Logic ---
            if event_name_col in df_uploaded.columns:
                initial_count = len(df_uploaded)
                banned_titles = ["TITOLO DI PROVA SIAE", "Verifica Conformità"]
                df_uploaded = df_uploaded[~df_uploaded[event_name_col].isin(banned_titles)]
                filtered_count = len(df_uploaded)
                if initial_count != filtered_count:
                    st.info(f"Escluse {initial_count - filtered_count} righe con titoli non validi ({', '.join(banned_titles)}).")

            st.subheader("Anteprima Dati Elaborati")
            st.dataframe(
                df_uploaded.head(),
                column_config={
                    "Incasso": st.column_config.NumberColumn("Incasso", format="%.2f €")
                }
            )
            
            st.subheader("Carica su Supabase")
            st.info(f"Tabella di destinazione: {DB_TABLE_NAME}")
            
            if st.button("Carica Dati su Supabase", key="btn_upload_eventi"):
                if not supabase:
                    st.error("Client Supabase non inizializzato. Controlla le credenziali.")
                else:
                    try:
                        # 1. Fetch Existing Data needed for Summation
                        # We need ID to update, and current values to sum
                        # Important: event_name_col contains the column name for the title (e.g. "Titolo Evento")
                        response = supabase.table(DB_TABLE_NAME).select("id, data_inizio, data_fine, \"Nr. Eventi\", autore, \"Tot. Presenze\", Incasso, \"" + event_name_col + "\"").execute()
                        existing_rows = response.data if response.data else []
                        
                        # Map: (Titolo) -> {id, pres, inc, start_date, end_date, nr_eventi}
                        existing_map = {}
                        for r in existing_rows:
                            t = r.get(event_name_col)
                            if t:
                                existing_map[t] = {
                                    'id': r['id'],
                                    'presenze': r.get('Tot. Presenze') or 0,
                                    'incasso': r.get('Incasso') or 0.0,
                                    'nr_eventi': r.get('Nr. Eventi') or 0,
                                    'data_inizio': r.get('data_inizio'),
                                    'data_fine': r.get('data_fine')
                                }

                        new_records = []
                        updated_count = 0
                        skipped_count = 0
                        
                        progress_bar = st.progress(0)
                        total_rows = len(df_uploaded)

                        for i, (index, row) in enumerate(df_uploaded.iterrows()):
                             # row_pres already numeric
                            row_title = row['Titolo Evento']
                            row_pres = pd.to_numeric(row.get('Tot. Presenze', 0), errors='coerce')
                            row_pres = 0 if pd.isna(row_pres) else row_pres
                            
                            row_inc = pd.to_numeric(row.get('Incasso', 0.0), errors='coerce')
                            row_inc = 0.0 if pd.isna(row_inc) else row_inc
                            
                            key = row_title

                            if key in existing_map:
                                db_rec = existing_map[key]
                                
                                # --- STRICT DUPLICATE RULE ---
                                db_start_str = str(db_rec.get('data_inizio', ''))
                                db_end_str = str(db_rec.get('data_fine', ''))
                                row_start_str = str(row['data_inizio'])
                                row_end_str = str(row['data_fine'])
                                
                                db_pres = int(db_rec.get('presenze', 0))
                                row_pres_val = int(row_pres)

                                # If Title (key), Start, End, and Presenze match exactly -> Skip
                                if (db_start_str == row_start_str) and \
                                   (db_end_str == row_end_str) and \
                                   (db_pres == row_pres_val):
                                    skipped_count += 1
                                    if i % 5 == 0: progress_bar.progress(min((i + 1) / total_rows, 1.0))
                                    continue 

                                # UPDATE Logic (Sum + Date Range Merge)
                                new_pres_val = db_rec['presenze'] + row_pres
                                new_inc_val = db_rec['incasso'] + row_inc
                                new_ev_val = db_rec['nr_eventi'] + row.get('Nr. Eventi', 0)
                                
                                # Date Comparison Logic
                                try:
                                    db_start = pd.to_datetime(db_rec['data_inizio']).date() if db_rec['data_inizio'] else None
                                    db_end = pd.to_datetime(db_rec['data_fine']).date() if db_rec['data_fine'] else None
                                    
                                    row_start = pd.to_datetime(row['data_inizio']).date()
                                    row_end = pd.to_datetime(row['data_fine']).date()
                                    
                                    # Handle Start
                                    if db_start:
                                        final_start = min(db_start, row_start)
                                    else:
                                        final_start = row_start
                                        
                                    # Handle End
                                    if db_end:
                                        final_end = max(db_end, row_end)
                                    else:
                                        final_end = row_end
                                        
                                except Exception:
                                    # Fallback
                                    final_start = row['data_inizio']
                                    final_end = row['data_fine']
                                
                                # Perform Update
                                supabase.table(DB_TABLE_NAME).update({
                                    "Tot. Presenze": int(new_pres_val),
                                    "Incasso": float(new_inc_val),
                                    "Nr. Eventi": int(new_ev_val),
                                    "data_inizio": str(final_start),
                                    "data_fine": str(final_end),
                                    "autore": row['autore'] # Keeping latest author logic
                                }).eq("id", db_rec['id']).execute()
                                
                                # Update local map
                                existing_map[key]['presenze'] = new_pres_val
                                existing_map[key]['incasso'] = new_inc_val
                                existing_map[key]['nr_eventi'] = new_ev_val
                                existing_map[key]['data_inizio'] = str(final_start)
                                existing_map[key]['data_fine'] = str(final_end)
                                
                                updated_count += 1
                            else:
                                # INSERT Logic
                                # Clean NaN for JSON
                                clean_row = row.where(pd.notnull(row), None).to_dict()
                                new_records.append(clean_row)
                            
                            # Update progress
                            if i % 5 == 0:
                                progress_bar.progress(min((i + 1) / total_rows, 1.0))
                        
                        progress_bar.progress(1.0)

                        # Bulk Insert New
                        if new_records:
                            supabase.table(DB_TABLE_NAME).insert(new_records).execute()
                        
                        st.success(f"Operazione completata! ✅ Inseriti: {len(new_records)} nuovi record. 🔄 Aggiornati (sommati): {updated_count} record esistenti.")

                    except Exception as e:
                        st.error(f"Errore durante l'elaborazione: {e}")
                        
        except Exception as e:
            st.error(f"Errore nella lettura del file: {e}")
    else:
        st.info("Carica un file Excel (ExportEventi) per iniziare.")

    # --- SECTION NEW: ExportEventi (Top/Flop) ---
    st.divider()
    st.subheader("Carica file ExportEventi.xlsx (Analisi Top/Flop)")
    
    # --- RESET TOOL ---
    with st.expander("⚠️ Gestione Dati Analisi (Reset)", expanded=False):
        st.warning("Usa questo pulsante se vuoi cancellare tutte le classifiche attuali e re-importare il file da zero.")
        if st.button("🗑️ Svuota intera tabella Analisi", type="primary", key="btn_truncate_highlights"):
            if not supabase:
                 st.error("DB non connesso.")
            else:
                try:
                    supabase.table("eventi_highlights").delete().neq("id", 0).execute()
                    st.success("Tabella svuotata. Ora puoi ricaricare il file Excel.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante lo svuotamento: {e}")

    uploaded_top_flop = st.file_uploader("Scegli ExportEventi.xlsx per Analisi", type=['xlsx'], key="upload_top_flop")

    if uploaded_top_flop:
        try:
            # Load specific sheet
            try:
                df_tf = pd.read_excel(uploaded_top_flop, sheet_name='export_eventi')
            except ValueError:
                st.error("Foglio 'export_eventi' non trovato.")
                st.stop()
            
            # Logic: Col A=Data (0), Col C=Titolo (2), Col K=Ingressi (10)
            if df_tf.shape[1] < 11:
                st.error("Il file non ha abbastanza colonne (serve almeno Colonna K).")
            else:
                top_flop_data = []
                
                for idx, row in df_tf.iterrows():
                    r_data = row.iloc[0]      # Col A
                    r_titolo = row.iloc[1]    # Col B
                    r_autore = row.iloc[2]    # Col C
                    r_nazione = row.iloc[3]   # Col D
                    r_ingressi = row.iloc[10] # Col K
                    r_incasso = row.iloc[12]  # Col M (Index 12) - Incasso
                    
                    try:
                        ing_val = float(r_ingressi)
                        if pd.isna(ing_val) or ing_val <= 0: continue
                        
                        inc_val = 0.0
                        try:
                            inc_val = float(r_incasso)
                        except:
                            inc_val = 0.0
                            
                    except: continue
                    
                    try:
                         d_parsed = pd.to_datetime(r_data, dayfirst=True)
                         if pd.isna(d_parsed): continue
                         
                         # NEW: Extract Time string (HH:MM)
                         time_str = d_parsed.strftime('%H:%M')
                         
                         # SUMMER FILTER (15/06 - 15/09)
                         md = (d_parsed.month, d_parsed.day)
                         if (6, 15) <= md <= (9, 15):
                             continue # Skip summer movies
                    except: continue
                        
                    top_flop_data.append({
                        "data": d_parsed.strftime('%Y-%m-%d'),
                        "orario": time_str,
                        "titolo_evento": str(r_titolo).strip(),
                        "autore": str(r_autore).strip() if pd.notna(r_autore) else "",
                        "nazione": str(r_nazione).strip() if pd.notna(r_nazione) else "",
                        "ingressi": int(ing_val),
                        "incasso": float(inc_val)
                    })
                
                df_clean = pd.DataFrame(top_flop_data)
                
                df_clean = pd.DataFrame(top_flop_data)
                
                if df_clean.empty:
                    st.warning("Nessun dato valido trovato.")
                else:
                    st.success(f"File processato: trovate {len(df_clean)} righe valide.")
                    
                    # Preview Data
                    st.subheader("Anteprima Dati (Nuovi)")
                    st.dataframe(df_clean.head(), hide_index=True)

                    # --- SAVING LOGIC (INCREMENTAL) ---
                    st.info("Tabella destinazione: eventi_highlights (Append mode)")
                    
                    if st.button("💾 Carica Dati (Incrementale)", type="primary"):
                        if not supabase:
                            st.error("DB non connesso.")
                        else:
                            try:
                                # Prepare ALL records (Let DB handle duplicates)
                                records_payload = []
                                
                                for _, row in df_clean.iterrows():
                                    records_payload.append({
                                        "data": row['data'],
                                        "orario": row['orario'],
                                        "titolo_evento": row['titolo_evento'],
                                        "autore": row['autore'],
                                        "nazione": row['nazione'],
                                        "ingressi": int(row['ingressi']),
                                        "incasso": float(row['incasso']),
                                        "categoria": "DATA", # Correct Italian column name
                                        "proiezioni_count": 1
                                    })
                                
                                # Perform UPSERT with Ignore Duplicates
                                if records_payload:
                                    # count='exact' helps us know if insertion happened, but upsert return is tricky with ignore.
                                    # We trust the operation.
                                    supabase.table("eventi_highlights").upsert(
                                        records_payload, 
                                        on_conflict="data, titolo_evento, orario", 
                                        ignore_duplicates=True
                                    ).execute()
                                    
                                    st.success(f"✅ Elaborazione completata! I dati sono stati uniti (Upsert). Se i numeri non tornano, prova a svuotare la tabella e ricaricare.")
                                else:
                                    st.warning("Nessun dato da elaborare.")
                                
                            except Exception as e:
                                st.error(f"Errore DB: {e}")
                                if "column" in str(e):
                                    st.info("Suggerimento: Controlla che le colonne 'incasso', 'categoria' esistano.")

        except Exception as e:
            st.error(f"Errore lettura file: {e}")

    # --- SECTION 2: ExportTitoliFiscali ---
    st.divider()
    st.subheader("Carica file ExportTitoliFiscali.xlsx")
    
    uploaded_fiscali = st.file_uploader("Scegli un file Excel (Fiscali)", type=['xlsx', 'xls'], key="fiscali_uploader")
    st.info("Tabella di destinazione: dettaglio_ingressi")

    if uploaded_fiscali is not None:
        try:
            # We use header=None because we rely on Excel A=0 index logic
            # Col C=2, H=7, V=21
            df_fiscali = pd.read_excel(uploaded_fiscali, header=None)
            
            if df_fiscali.shape[1] < 22:
                st.error("Il file non ha abbastanza colonne (richiesta almeno Colonna V / Indice 21).")
            else:
                agg_map = {}
                
                # Iterate rows
                for idx, row in df_fiscali.iterrows():
                    # Parse Date (Col C / Index 2)
                    # Parse Date (Col C / Index 2)
                    raw_val = row.iloc[2]
                    dt_obj = None

                    try:
                        if pd.isnull(raw_val):
                            continue
                            
                        # Case 1: Already a datetime/timestamp object (Pandas auto-conversion)
                        if isinstance(raw_val, (datetime, pd.Timestamp)):
                            dt_obj = raw_val
                        # Case 2: String - try specific format first, then generic
                        elif isinstance(raw_val, str):
                            raw_val = raw_val.strip()
                            try:
                                # Specific format requested by user
                                dt_obj = datetime.strptime(raw_val, "%d/%m/%Y %H.%M.%S")
                            except ValueError:
                                # Fallback for standard formats
                                dt_obj = pd.to_datetime(raw_val, dayfirst=True)
                                
                    except Exception:
                        continue # Skip row if date is unparseable

                    if dt_obj is None:
                        continue
                    
                    # Parse Title (Col H / Index 7)
                    title = row.iloc[7]
                    if pd.isna(title):
                        continue
                    
                    # Parse TicketType (Col V / Index 21)
                    ticket_val = str(row.iloc[21]).strip()
                    
                    # Extract time string (HH:MM)
                    time_str = dt_obj.strftime('%H:%M')
                    
                    # New Key: (Title, Date, Time)
                    key = (title, dt_obj.date(), time_str)
                    
                    if key not in agg_map:
                        agg_map[key] = {
                            "titolo_evento": title,
                            "data": dt_obj.date(),
                            "orario": time_str,
                            "first_dt": dt_obj, # for get_evento
                            "interi": 0, "ridotti": 0, "soci": 0, "omaggio": 0, "nc": 0
                        }
                    
                    stats = agg_map[key]
                    if ticket_val == 'R7':
                        stats['ridotti'] += 1
                    elif ticket_val == 'I1':
                        stats['interi'] += 1
                    elif ticket_val == 'O7':
                        stats['omaggio'] += 1
                    elif ticket_val == 'R8':
                        stats['soci'] += 1
                    else:
                        stats['nc'] += 1
                
                if not agg_map:
                    st.warning("Nessuna riga valida trovata (controlla il formato data 'dd/mm/yyyy H.M.S').")
                else:
                    st.success(f"File elaborato! Trovati {len(agg_map)} eventi aggregati.")
                    
                    if st.button("Carica Dati Fiscali su Supabase", key="btn_upload_fiscali"):
                        if not supabase:
                             st.error("Client Supabase non connesso.")
                        else:
                             try:
                                 # 1. Fetch Existing Keys (Pre-processing)
                                 existing_records = set()
                                 try:
                                     response = supabase.table("dettaglio_ingressi").select("data, evento").execute()
                                     if response.data:
                                         existing_records = {(row['data'], row['evento']) for row in response.data}
                                 except Exception as e_fetch:
                                     st.warning(f"Attenzione: Impossibile scaricare dati esistenti per controllo duplicati ({e_fetch}).")

                                 # 2. Get Max ID
                                 next_id = 1
                                 try:
                                     res_id = supabase.table("dettaglio_ingressi").select("id").order("id", desc=True).limit(1).execute()
                                     if res_id.data:
                                         next_id = res_id.data[0]['id'] + 1
                                 except Exception as e:
                                     st.warning(f"Attenzione: Impossibile recuperare ultimo ID ({e}). Parto da 1.")
                                 
                                 # 3. Prepare Insert
                                 records_insert = []
                                 skipped_count = 0

                                 for k, v in agg_map.items():
                                     evt_tag = get_evento(v['first_dt'])
                                     final_tag = evt_tag if evt_tag else "adu"
                                     date_str = v['data'].strftime('%Y-%m-%d')
                                     
                                     # Duplicate Check
                                     if (date_str, final_tag) in existing_records:
                                         skipped_count += 1
                                         continue

                                     rec = {
                                         "id": next_id,
                                         "data": date_str,
                                         "orario": v['orario'],
                                         "evento": final_tag,
                                         "interi": v['interi'],
                                         "ridotti": v['ridotti'],
                                         "soci": v['soci'],
                                         "omaggio": v['omaggio'],
                                         "nc": v['nc']
                                     }
                                     records_insert.append(rec)
                                     next_id += 1
                                 
                                 # 4. Insert
                                 if records_insert:
                                    supabase.table("dettaglio_ingressi").insert(records_insert).execute()
                                    st.success(f"Importazione completata: {len(records_insert)} nuovi ingressi inseriti ({skipped_count} saltati perché già presenti).")
                                 else:
                                    if skipped_count > 0:
                                        st.warning(f"Nessun dato inserito. {skipped_count} record saltati perché già presenti.")
                                    else:
                                        st.info("Nessun dato da inserire.")
                                     
                             except Exception as ex:
                                 st.error(f"Errore durante l'inserimento: {ex}")

        except Exception as e:
            st.error(f"Errore generale: {e}")


# Funzione helper per le Card KPI
def render_kpi_card(title, value, comparison_html, details, border_color="gray", prev_value=None):
    comp_content = comparison_html
    if prev_value:
        comp_content = f"{comparison_html} <span style='font-size: 13px; color: #666;'>(vs {prev_value})</span>"

    st.markdown(f"""
<div style="background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 5px solid {border_color};">
    <div style="font-size: 12px; font-weight: bold; color: #888; text-transform: uppercase; letter-spacing: 1px;">
        {title}
    </div>
    <div style="font-size: 36px; font-weight: bold; color: #333; margin: 10px 0;">
        {value}
    </div>
    <div style="font-size: 14px; margin-bottom: 8px;">
        {comp_content}
    </div>
    <div style="font-size: 13px; color: #666;">
        {details}
    </div>
</div>
""", unsafe_allow_html=True)

# Funzione per generare dati mock per il Periodo 2
def generate_mock_data(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)
    data = []
    
    for date in dates:
        # Randomly decide if there are events today
        if np.random.random() > 0.3: # 70% chance of events
            num_events = np.random.randint(1, 5)
            for _ in range(num_events):
                evento_type = np.random.choice(['adu', 'ven', 'bam'], p=[0.6, 0.2, 0.2])
                incasso = np.random.uniform(100, 1000)
                presenze = int(incasso / np.random.uniform(5, 15))
                
                data.append({
                    'data_inizio': date.strftime('%Y-%m-%d'),
                    'Evento': evento_type,
                    'Incasso': incasso,
                    'Presenze': presenze,
                    'VOS': np.random.choice([True, False], p=[0.1, 0.9])
                })
    
    return pd.DataFrame(data)

# Funzione per calcolare le metriche
def calculate_metrics(df):
    kpi_configs = {
        'TOTALE': {'filter': None, 'color': 'blue'},
        'ADULTI': {'filter': 'adu', 'color': 'green'},
        'VENERDÌ': {'filter': 'ven', 'color': 'red'},
        'BAMBINI': {'filter': 'bam', 'color': 'orange'}
    }
    
    metrics = {}
    for key, config in kpi_configs.items():
        if config['filter']:
            subset = df[df['Evento'] == config['filter']]
        else:
            subset = df
        
        total_incasso = subset['Incasso'].sum() if 'Incasso' in subset.columns else 0
        tot_presenze = 0
        if 'Tot. Presenze' in subset.columns:
            tot_presenze = pd.to_numeric(subset['Tot. Presenze'], errors='coerce').fillna(0).sum()
        elif 'Presenze' in subset.columns:
            tot_presenze = pd.to_numeric(subset['Presenze'], errors='coerce').fillna(0).sum()
        else:
            tot_presenze = len(subset)

        avg_ticket = total_incasso / tot_presenze if tot_presenze > 0 else 0
        count_events = len(subset)
        
        metrics[key] = {
            'incasso': total_incasso,
            'presenze': tot_presenze,
            'avg_ticket': avg_ticket,
            'count': count_events,
            'color': config['color']
        }
    return metrics

# Helper to get max date
def get_latest_date():
    try:
        res = supabase.table(DB_TABLE_NAME).select("data_fine").order("data_fine", desc=True).limit(1).execute()
        if res.data and len(res.data) > 0:
            return res.data[0]['data_fine']
    except:
        return None
    return None

# Funzione per la pagina di Consultazione
def render_consulta_page():
    # CSS Injection per stile globale e fix layout
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap');
        
        /* Background App */
        .stApp {
            background-color: #F0F2F6;
            font-family: 'Roboto', sans-serif;
        }
        
        /* Header H3 pulito */
        h3 {
            color: #202124;
            font-weight: 500;
            padding-top: 20px;
        }
        
        /* Rimuovi padding eccessivo standard di Streamlit */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* Nascondi elementi standard */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

    st.title("📊 Dashboard Analitica")

    # Display Last Update
    last_upd = get_latest_date()
    if last_upd:
        try:
            # Assuming standard YYYY-MM-DD from Supabase
            dt_last = datetime.strptime(last_upd, "%Y-%m-%d")
            st.caption(f"📅 Dati aggiornati fino al {dt_last.strftime('%d/%m/%Y')}")
        except:
            pass

    global supabase

    if not supabase:
        st.error("Errore: Client Supabase non inizializzato. Configura le credenziali nella pagina 'Importa Dati' o in `secrets.toml`.")
        return

    # Helper per formattazione Euro
    def format_euro(amount):
        return f"€ {amount:,.0f}".replace(",", ".") 

    def format_euro_precise(amount):
        return f"€ {amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # --- Top Control Bar ---
    with st.container():
        # Toggle Comparison
        col_toggle, _ = st.columns([1, 3])
        with col_toggle:
            comparison_mode = st.toggle("Confronta con altro periodo")

        start_date_p2 = None
        end_date_p2 = None

        if comparison_mode:
            col_period1, col_period2, col_button = st.columns([2, 2, 1])
            
            with col_period1:
                st.markdown("### Periodo 1")
                c1, c2 = st.columns(2)
                with c1:
                    start_date = st.date_input("Da", value=datetime.now() - timedelta(days=30), format="DD/MM/YYYY", key="p1_start")
                with c2:
                    end_date = st.date_input("A", value=datetime.now(), format="DD/MM/YYYY", key="p1_end")
            
            with col_period2:
                st.markdown("### Periodo 2")
                c1, c2 = st.columns(2)
                # Default Period 2 to previous year same dates
                default_p2_start = start_date - timedelta(days=365)
                default_p2_end = end_date - timedelta(days=365)
                with c1:
                    start_date_p2 = st.date_input("Da", value=default_p2_start, format="DD/MM/YYYY", key="p2_start")
                with c2:
                    end_date_p2 = st.date_input("A", value=default_p2_end, format="DD/MM/YYYY", key="p2_end")
            
            with col_button:
                st.markdown("### &nbsp;") # Spacer for Header
                st.write("") # Spacer for Label
                elabora_btn = st.button("ELABORA", type="primary", use_container_width=True)

        else:
            col_date1, col_date2, col_button = st.columns([2, 2, 1])
            with col_date1:
                start_date = st.date_input("Da Data", value=datetime.now() - timedelta(days=30), format="DD/MM/YYYY")
            with col_date2:
                end_date = st.date_input("A Data", value=datetime.now(), format="DD/MM/YYYY")
                
            with col_button:
                st.write("") # Spacer for Label
                st.write("") 
                elabora_btn = st.button("ELABORA", type="primary", use_container_width=True)

    # Inizializza session state con STRICT ISOLATION names
    if 'df_main' not in st.session_state:
        st.session_state.df_main = None
    if 'df_compare' not in st.session_state:
        st.session_state.df_compare = None
    if 'detail_df_main' not in st.session_state:
        st.session_state.detail_df_main = None
    if 'detail_df_compare' not in st.session_state:
        st.session_state.detail_df_compare = None
    if 'active_filter' not in st.session_state:
        st.session_state.active_filter = 'TOTALE'
    if 'is_comparison' not in st.session_state:
        st.session_state.is_comparison = False

    # Sidebar settings
    st.sidebar.subheader("Impostazioni")

    # Logica Elaborazione
    if elabora_btn:
        st.session_state.is_comparison = comparison_mode
        try:
            # ==============================================================================
            # STEP 1: FETCH PERIOD 1 (MAIN)
            # ==============================================================================
            # A. Main Events Table
            response_main = supabase.table(DB_TABLE_NAME) \
                .select("*") \
                .gte("data_inizio", start_date.strftime('%Y-%m-%d')) \
                .lte("data_inizio", end_date.strftime('%Y-%m-%d')) \
                .execute()
            
            if response_main.data:
                _df = pd.DataFrame(response_main.data)
                if 'Incasso' in _df.columns:
                    _df['Incasso'] = pd.to_numeric(_df['Incasso'], errors='coerce').fillna(0)
                st.session_state.df_main = _df
            else:
                st.session_state.df_main = pd.DataFrame()
            
            # Safe Initialization Main
            if st.session_state.df_main.empty:
                st.session_state.df_main = pd.DataFrame(columns=["data_inizio", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento", "Nr. Eventi"])

            # B. Detail Table (dettaglio_ingressi)
            try:
                response_detail_main = supabase.table("dettaglio_ingressi") \
                    .select("*") \
                    .gte("data", start_date.strftime('%Y-%m-%d')) \
                    .lte("data", end_date.strftime('%Y-%m-%d')) \
                    .execute()
                
                if response_detail_main.data:
                    st.session_state.detail_df_main = pd.DataFrame(response_detail_main.data)
                else:
                    st.session_state.detail_df_main = pd.DataFrame()
            except Exception as e:
                print(f"Errore dettaglio_ingressi P1: {e}")
                st.session_state.detail_df_main = pd.DataFrame()

            # Safe Initialization Detail Main
            if st.session_state.detail_df_main.empty:
                st.session_state.detail_df_main = pd.DataFrame(columns=["interi", "ridotti", "soci", "omaggio", "nc", "evento"])

            # ==============================================================================
            # STEP 2: FETCH PERIOD 2 (COMPARISON) - ONLY IF TOGGLE IS ON
            # ==============================================================================
            # Pre-initialize to avoid UnboundLocalError or previous state leaks
            detail_df_compare = pd.DataFrame() 

            if comparison_mode and start_date_p2 and end_date_p2:
                # A. Main Events Table (Comparison)
                try:
                    response_compare = supabase.table(DB_TABLE_NAME) \
                        .select("*") \
                        .gte("data_inizio", start_date_p2.strftime('%Y-%m-%d')) \
                        .lte("data_inizio", end_date_p2.strftime('%Y-%m-%d')) \
                        .execute()
                    
                    if response_compare.data:
                        _df_cmp = pd.DataFrame(response_compare.data)
                        if 'Incasso' in _df_cmp.columns:
                            _df_cmp['Incasso'] = pd.to_numeric(_df_cmp['Incasso'], errors='coerce').fillna(0)
                        st.session_state.df_compare = _df_cmp
                    else:
                        st.session_state.df_compare = pd.DataFrame()
                    
                    # Safe Initialization Compare
                    if st.session_state.df_compare.empty:
                        st.session_state.df_compare = pd.DataFrame(columns=["data_inizio", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento", "Nr. Eventi"])

                except Exception as e:
                     st.error(f"Errore recupero dati Periodo 2: {e}")
                     st.session_state.df_compare = pd.DataFrame(columns=["data_inizio", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento", "Nr. Eventi"])

                # B. Detail Table (Comparison)
                try:
                    response_detail_cmp = supabase.table("dettaglio_ingressi") \
                        .select("*") \
                        .gte("data", start_date_p2.strftime('%Y-%m-%d')) \
                        .lte("data", end_date_p2.strftime('%Y-%m-%d')) \
                        .execute()
                    
                    if response_detail_cmp.data:
                         st.session_state.detail_df_compare = pd.DataFrame(response_detail_cmp.data)
                    else:
                         st.session_state.detail_df_compare = pd.DataFrame()
                except Exception as e:
                    print(f"Errore dettaglio_ingressi P2: {e}")
                    st.session_state.detail_df_compare = pd.DataFrame()

                # Safe Initialization Detail Compare
                if st.session_state.detail_df_compare.empty:
                    st.session_state.detail_df_compare = pd.DataFrame(columns=["interi", "ridotti", "soci", "omaggio", "nc", "evento"])

            else:
                # Reset Comparison Data if mode is off
                st.session_state.df_compare = None
                st.session_state.detail_df_compare = None
                
            st.session_state.active_filter = 'TOTALE'
            
        except Exception as e:
            st.error(f"Errore durante il recupero dei dati: {e}")
            st.session_state.df_main = pd.DataFrame(columns=["data_inizio", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento", "Nr. Eventi"])
            st.session_state.df_compare = None

    # --- KPI Section ---
    if st.session_state.df_main is not None and not st.session_state.df_main.empty:
        # STRICT ISOLATION: Local variables for calculation
        df_main = st.session_state.df_main
        metrics_p1 = calculate_metrics(df_main)
        
        metrics_p2 = None
        if st.session_state.is_comparison and st.session_state.df_compare is not None:
            df_compare = st.session_state.df_compare
            metrics_p2 = calculate_metrics(df_compare)

        # Render KPI Cards
        st.markdown("###") # Spacer
        cols = st.columns(4)
        
        order = ['TOTALE', 'ADULTI', 'VENERDÌ', 'BAMBINI']
        
        for i, key in enumerate(order):
            m1 = metrics_p1[key]
            
            comparison_html = "&nbsp;"
            prev_value_formatted = None

            if metrics_p2:
                m2 = metrics_p2[key]
                val1 = m1['incasso']
                val2 = m2['incasso']
                
                prev_value_formatted = format_euro(val2)

                if val2 != 0:
                    delta_percent = ((val1 - val2) / val2) * 100
                elif val1 != 0:
                    delta_percent = 100.0 
                else:
                    delta_percent = 0.0
                
                if delta_percent > 0:
                    color = "#28a745"
                    arrow = "▲"
                elif delta_percent < 0:
                    color = "#dc3545"
                    arrow = "▼"
                else:
                    color = "#6c757d"
                    arrow = "•"
                
                comparison_html = f'<span style="color: {color}; font-weight: bold;">{arrow} {delta_percent:+.1f}%</span>'

            presenze_formatted = f"{int(m1['presenze']):,}".replace(",", ".")
            avg_ticket_formatted = format_euro_precise(m1['avg_ticket'])
            details = f"<strong>{presenze_formatted}</strong> Presenze<br>Biglietto medio: <strong>{avg_ticket_formatted}</strong> • {m1['count']} Eventi"

            with cols[i]:
                render_kpi_card(
                    title=key,
                    value=format_euro(m1['incasso']),
                    comparison_html=comparison_html,
                    details=details,
                    border_color=m1['color'],
                    prev_value=prev_value_formatted
                )

        # --- Filter Selection & Data Table (Only if NOT comparison) ---
        if not st.session_state.is_comparison:
            st.markdown("###")
            st.markdown("### Dettaglio Analitico")
            
            # Filtro visivo
            col_filter, col_metric = st.columns([3, 1])
            
            with col_filter:
                st.radio(
                    "Filtra Tabella:",
                    options=order,
                    horizontal=True,
                    label_visibility="collapsed",
                    key="active_filter"
                )

            # Metrica Dinamica
            kpi_configs_metric = { 
                'TOTALE': {'filter': None},
                'ADULTI': {'filter': 'adu'},
                'VENERDÌ': {'filter': 'ven'},
                'BAMBINI': {'filter': 'bam'}
            }
            
            current_config_metric = kpi_configs_metric[st.session_state.active_filter]
            if current_config_metric['filter']:
                subset_metric = df_main[df_main['Evento'] == current_config_metric['filter']]
            else:
                subset_metric = df_main
            
            sum_presenze = 0
            if 'Tot. Presenze' in subset_metric.columns:
                sum_presenze = pd.to_numeric(subset_metric['Tot. Presenze'], errors='coerce').fillna(0).sum()
            elif 'Presenze' in subset_metric.columns:
                sum_presenze = pd.to_numeric(subset_metric['Presenze'], errors='coerce').fillna(0).sum()
                
            sum_eventi = 0
            if 'Nr. Eventi' in subset_metric.columns:
                 sum_eventi = pd.to_numeric(subset_metric['Nr. Eventi'], errors='coerce').fillna(0).sum()
            else:
                 sum_eventi = len(subset_metric)

            media_ingressi = sum_presenze / sum_eventi if sum_eventi > 0 else 0

            with col_metric:
                st.metric(label="Media Ingressi", value=f"{media_ingressi:.2f}")
            
            # Dataframe Display
            # Define styling configurations for the filters
            kpi_configs = {
                "TOTALE": {"label": "TOTALE", "color": "#0d6efd", "filter": None},   # Blue
                "ADULTI": {"label": "ADULTI", "color": "#198754", "filter": "adu"},   # Green
                "VENERDÌ": {"label": "VENERDÌ", "color": "#dc3545", "filter": "ven"}, # Red
                "BAMBINI": {"label": "BAMBINI", "color": "#fd7e14", "filter": "bam"}  # Orange
            }
            
            current_config = kpi_configs[st.session_state.active_filter]
            if current_config['filter']:
                display_df = df_main[df_main['Evento'] == current_config['filter']].copy()
            else:
                display_df = df_main.copy()

            if 'VOS' in display_df.columns and display_df['VOS'].any():
                if st.checkbox("Mostra solo Versione Originale (VOS)", value=False):
                    display_df = display_df[display_df['VOS'] == True]

            # 1. Hide unwanted columns
            cols_to_drop = ['id', 'Evento', 'VOS', 'temp_datetime', 'RASSEGNA', 'tmdb_processed']
            display_df = display_df.drop(columns=[c for c in cols_to_drop if c in display_df.columns])

            # 2. Reorder Columns (Autore after Title)
            desired_order = ['Titolo Evento', 'autore', 'Nr. Eventi', 'Nazionalità', 'Tot. Presenze', 'Incasso', 'data_inizio', 'data_fine']
            # Select only existing columns from desired_order + any others not mentioned
            final_cols = [c for c in desired_order if c in display_df.columns] + [c for c in display_df.columns if c not in desired_order]
            display_df = display_df[final_cols]

            # 3. Rename & Format
            column_config = {
                "data_inizio": st.column_config.DateColumn("Data inizio", format="DD/MM/YYYY"),
                "data_fine": st.column_config.DateColumn("Data fine", format="DD/MM/YYYY"),
                "Incasso": st.column_config.NumberColumn("Incasso", format="%.2f €"),
            }
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config=column_config
            )

        # --- PIE CHARTS SECTION (Refactored) ---
        # Helper interno per i grafici
        def render_pie_chart_helper(data_df, chart_type, title, active_filter=None):
            if data_df is None or data_df.empty:
                st.info(f"Nessun dato per {title}")
                return

            if chart_type == "nationality":
                if 'Nazionalità' not in data_df.columns or 'Incasso' not in data_df.columns:
                    st.info("Colonne mancanti per Nazionalità.")
                    return
                # Data Prep
                pie_df = data_df.copy()
                pie_df['Incasso'] = pd.to_numeric(pie_df['Incasso'], errors='coerce').fillna(0)
                pie_grouped = pie_df.groupby('Nazionalità', as_index=False)['Incasso'].sum()
                total_incasso = pie_grouped['Incasso'].sum()
                
                if total_incasso <= 0:
                    st.info(f"Totale incassi 0 per {title}.")
                    return

                # Altri < 5%
                pie_grouped['Nazionalità'] = pie_grouped.apply(
                    lambda x: x['Nazionalità'] if (x['Incasso'] / total_incasso) >= 0.05 else 'Altri', 
                    axis=1
                )
                pie_final = pie_grouped.groupby('Nazionalità', as_index=False)['Incasso'].sum()
                pie_final = pie_final.sort_values(by='Incasso', ascending=False).head(10)
                
                fig = px.pie(pie_final, values='Incasso', names='Nazionalità', title=title)
                fig.update_traces(textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)

            elif chart_type == "tickets":
                # Filter Logic
                current_filter = active_filter if active_filter else 'TOTALE'
                kpi_configs = { 
                    'TOTALE': {'filter': None},
                    'ADULTI': {'filter': 'adu'},
                    'VENERDÌ': {'filter': 'ven'},
                    'BAMBINI': {'filter': 'bam'}
                }
                filter_val = kpi_configs.get(current_filter, {}).get('filter')

                df_filtered = data_df.copy()
                if filter_val and 'evento' in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered['evento'] == filter_val]
                
                if df_filtered.empty:
                    st.info(f"Nessun dato ingressi per {title}")
                    return

                cols_to_sum = ['interi', 'ridotti', 'soci', 'omaggio']
                for c in cols_to_sum:
                    if c in df_filtered.columns:
                        df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)
                
                totals = df_filtered[cols_to_sum].sum()
                pie_data = pd.DataFrame({'Tipologia': totals.index.str.capitalize(), 'Totale': totals.values})
                pie_data = pie_data[pie_data['Totale'] > 0]
                
                if not pie_data.empty:
                    fig = px.pie(pie_data, values='Totale', names='Tipologia', title=title)
                    fig.update_traces(textposition='auto', textinfo='label+percent')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"Nessun ingresso > 0 per {title}")


        # Rendering Charts
        st.markdown("### Analisi Dettagliata")
        
        # 1. Nazionalità
        if st.session_state.is_comparison:
            st.subheader("Ripartizione Incassi per Nazionalità")
            c1, c2 = st.columns(2)
            with c1:
                # STRICT ISOLATION: P1 uses df_main
                if not st.session_state.df_main.empty and 'Nazionalità' in st.session_state.df_main.columns:
                    render_pie_chart_helper(st.session_state.df_main, "nationality", "Periodo 1")
                else:
                    st.info("Dati insufficienti per Nazionalità (P1)")
            with c2:
                # STRICT ISOLATION: P2 uses df_compare
                if st.session_state.df_compare is not None and not st.session_state.df_compare.empty and 'Nazionalità' in st.session_state.df_compare.columns:
                    render_pie_chart_helper(st.session_state.df_compare, "nationality", "Periodo 2")
                else:
                    st.info("Dati insufficienti per Nazionalità (P2)")
        else:
            # Single Mode - Nazionalità
             pass

        if not st.session_state.is_comparison:
             # Original Layout: Side by Side (Nat | Tickets)
             c_single1, c_single2 = st.columns(2)
             with c_single1:
                 if not st.session_state.df_main.empty and 'Nazionalità' in st.session_state.df_main.columns:
                    render_pie_chart_helper(st.session_state.df_main, "nationality", "Ripartizione Incassi per Nazionalità")
                 else:
                    st.info("Dati insufficienti per Nazionalità")

             with c_single2:
                 # Pass active_filter from session state
                 if not st.session_state.detail_df_main.empty and 'interi' in st.session_state.detail_df_main.columns:
                     render_pie_chart_helper(st.session_state.detail_df_main, "tickets", "Distribuzione Ingressi", st.session_state.active_filter)
                 else:
                     st.info("Dati insufficienti per Ingressi")
        
        else:
            # Comparison Mode - Tickets
            st.markdown("---")
            st.subheader("Distribuzione Ingressi")
            c3, c4 = st.columns(2)
            with c3:
                # STRICT ISOLATION: P1 uses detail_df_main
                if not st.session_state.detail_df_main.empty and 'interi' in st.session_state.detail_df_main.columns:
                    render_pie_chart_helper(st.session_state.detail_df_main, "tickets", "Ingressi (Periodo 1)")
                else:
                    st.info("Dati insufficienti per Ingressi (P1)")
            with c4:
                # STRICT ISOLATION: P2 uses detail_df_compare
                if st.session_state.detail_df_compare is not None and not st.session_state.detail_df_compare.empty and 'interi' in st.session_state.detail_df_compare.columns:
                    render_pie_chart_helper(st.session_state.detail_df_compare, "tickets", "Ingressi (Periodo 2)")
                else:
                     st.info("Dati insufficienti per Ingressi (P2)")
            
    elif st.session_state.df_main is not None and st.session_state.df_main.empty:
        st.info("Nessun dato disponibile.")
    else:
        st.info("Seleziona le date e clicca 'ELABORA' per iniziare.")

# Funzione per la pagina Utenti (RBAC Manager)
def get_entra_users():
    """
    Fetch users from Microsoft Entra ID (Graph API).
    Returns a list of dicts: {'val': email, 'label': 'Name (email)'}
    """
    # 1. Get Secrets
    ms_config = st.secrets.get("microsoft", {})
    CLIENT_ID = ms_config.get("client_id")
    CLIENT_SECRET = ms_config.get("client_secret")
    TENANT_ID = ms_config.get("tenant_id")
    
    if not (CLIENT_ID and CLIENT_SECRET and TENANT_ID):
        st.error("Missing Microsoft Credentials")
        return []

    # 2. Authenticate (App Context)
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    if "access_token" not in result:
        st.error(f"Graph Auth Error: {result.get('error_description')}")
        return []
        
    token = result['access_token']
    
    # 3. Query Graph API
    headers = {'Authorization': f'Bearer {token}'}
    # Top 999 to get all (for now)
    url = "https://graph.microsoft.com/v1.0/users?$select=displayName,mail,userPrincipalName&$top=999"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        users_list = []
        for u in data.get('value', []):
            email = u.get('mail') or u.get('userPrincipalName')
            name = u.get('displayName', 'No Name')
            if email:
                label = f"{name} ({email})"
                users_list.append({'email': email, 'label': label})
        
        # Sort by Name
        users_list.sort(key=lambda x: x['label'])
        return users_list
        
    except Exception as e:
        st.error(f"Graph API Error: {e}")
        return []

def render_users_page():
    st.title("👥 Gestione Utenti (RBAC)")
    
    global supabase
    if not supabase:
        st.error("Supabase non connesso.")
        return

    # 1. Add New User Form
    with st.expander("➕ Aggiungi / Autorizza Utente", expanded=True):
        
        # Fetch Entra Users
        entra_users = get_entra_users()
        
        with st.form("add_user_form"):
            # Select User from Graph
            selected_user = st.selectbox(
                "Seleziona Utente Microsoft", 
                options=entra_users, 
                format_func=lambda x: x['label'],
                index=None,
                placeholder="Cerca utente..."
            )
            
            new_role = st.selectbox("Assegna Ruolo", ["Base", "Gestione Turni", "Amministratore"])
            
            submitted = st.form_submit_button("Autorizza Utente")
            if submitted:
                if not selected_user:
                    st.error("Seleziona un utente.")
                else:
                    new_email = selected_user['email']
                    try:
                        # Upsert based on email
                        data = {"email": new_email, "role": new_role}
                        supabase.table("authorized_users").upsert(data, on_conflict="email").execute()
                        st.success(f"Utente {new_email} autorizzato come {new_role}.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Errore durante l'inserimento: {e}")

    # 2. List Users
    st.divider()
    st.subheader("Utenti Autorizzati")
    
    try:
        response = supabase.table("authorized_users").select("*").order("email").execute()
        users = response.data if response.data else []
        
        if not users:
            st.info("Nessun utente autorizzato trovato.")
        else:
            # Table Header
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.markdown("**Email**")
            c2.markdown("**Ruolo**")
            c3.markdown("**Azioni**")
            
            for user in users:
                c1, c2, c3 = st.columns([3, 2, 1])
                u_email = user.get('email')
                u_role = user.get('role', 'Visitatore')
                u_id = user.get('id') # If exists, useful for delete. If PK is email, use email.
                
                with c1:
                    st.write(u_email)
                with c2:
                    # Badge style
                    if u_role == 'Amministratore':
                        st.markdown(f"<span style='background-color: #d1e7dd; color: #0f5132; padding: 2px 8px; border-radius: 4px; font-size: 0.85em;'>{u_role}</span>", unsafe_allow_html=True)
                    else:
                         st.markdown(f"<span style='background-color: #f8f9fa; color: #6c757d; padding: 2px 8px; border-radius: 4px; font-size: 0.85em;'>{u_role}</span>", unsafe_allow_html=True)
                with c3:
                    if st.button("🗑️", key=f"del_{u_email}", help=f"Rimuovi {u_email}"):
                        try:
                            # Delete by email logic
                            supabase.table("authorized_users").delete().eq("email", u_email).execute()
                            st.success("Rimosso.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore rimozione: {e}")
                            
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
             st.warning("Tabella `authorized_users` non trovata. Creala in Supabase (colonne: email, role).")
        else:
             st.error(f"Errore recupero utenti: {e}")

# Main App Navigation
# Funzione per la pagina Riepiloghi
def render_riepiloghi_page():
    st.title("📈 Riepiloghi")
    
    global supabase
    if not supabase:
        st.error("Client Supabase non inizializzato.")
        return

    # 1. Fetch Data
    try:
        with st.spinner("Elaborazione riepiloghi in corso..."):
            # Fetch data: New column name "Tot. Presenze"
            # Note: Filter in Python if simple filter prevents proper sums, but SQL filter is better for performance
            response = supabase.table(DB_TABLE_NAME).select('data_inizio, "Tot. Presenze", Evento, Incasso').neq('"Tot. Presenze"', 0).not_.is_('"Tot. Presenze"', "null").execute()
            
            if not response.data:
                st.warning("Nessun dato disponibile.")
                return

            df = pd.DataFrame(response.data)
            if not df.empty:
                # Standardize column names for easier processing
                # Map "data_inizio" -> "data" AND "Tot. Presenze" -> "presenze"
                df.rename(columns={
                    'data_inizio': 'data', 
                    'Tot. Presenze': 'presenze',
                    'Evento': 'evento',
                    'Incasso': 'incasso'
                }, inplace=True)
            
            # Convert presenze to numeric just in case
            df['presenze'] = pd.to_numeric(df['presenze'], errors='coerce').fillna(0)
            df['incasso'] = pd.to_numeric(df['incasso'], errors='coerce').fillna(0)
            
            # 2. Anno Sociale Logic
            df['data'] = pd.to_datetime(df['data'])
            
            # Keep a copy for Summer Chart (since Anno Sociale logic drops summer months)
            df_full = df.copy()
            
            def get_anno_sociale(dt):
                if pd.isna(dt):
                    return None
                year = dt.year
                month = dt.month
                
                # Settembre (9) - Dicembre (12) -> E.g. 2023 -> "2023-24"
                if 9 <= month <= 12:
                    return f"{year}-{str(year+1)[-2:]}"
                # Gennaio (1) - Maggio (5) -> E.g. 2024 -> "2023-24" (Year-1)
                elif 1 <= month <= 5:
                    return f"{year-1}-{str(year)[-2:]}"
                # Giugno (6) - Agosto (8) -> Exclude
                else:
                    return None

            df['anno_sociale'] = df['data'].apply(get_anno_sociale)
            
            # --- PROCESS SUMMER DATA (Logic moved up for Spinner scope) ---
            # Filter: Month 7, 8 OR (Month 9 AND Day <= 10)
            mask_summer = (
                (df_full['data'].dt.month == 7) |
                (df_full['data'].dt.month == 8) |
                ((df_full['data'].dt.month == 9) & (df_full['data'].dt.day <= 10))
            )
            df_summer = df_full[mask_summer].copy()
            
            df_summer_grouped = pd.DataFrame() # Initialize
            if not df_summer.empty:
                # Create 'anno_solare' (YYYY)
                df_summer['anno_solare'] = df_summer['data'].dt.year
                
                # Group by Year ONLY (No event distinction)
                df_summer_grouped = df_summer.groupby(['anno_solare'])[['presenze', 'incasso']].sum().reset_index()
                df_summer_grouped = df_summer_grouped.sort_values('anno_solare')
                
                # Calculate Average Price
                df_summer_grouped['media_prezzo'] = df_summer_grouped.apply(
                    lambda x: x['incasso'] / x['presenze'] if x['presenze'] > 0 else 0, axis=1
                )
                
                # Formatter helpers
                def format_it_currency(val):
                    return f"€ {val:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

                def format_it_thousands(val):
                    return f"€ {val:,.0f}".replace(',', '.')
                
                # Create Custom Labels
                df_summer_grouped['x_label'] = df_summer_grouped.apply(
                    lambda x: f"{int(x['anno_solare'])}<br>(Biglietto: {format_it_currency(x['media_prezzo'])})", 
                    axis=1
                )
                df_summer_grouped['bar_text'] = df_summer_grouped.apply(
                    lambda x: format_it_thousands(x['incasso']), 
                    axis=1
                )

            # Filter out summer months (None) for Main Chart
            df = df.dropna(subset=['anno_sociale'])
            
            if df.empty:
                 st.warning("Nessun dato valido per gli Anni Sociali (Esclusi mesi 6-8).")
                 return

            # 3. Grouping Main
            # Rename Events for Display
            df['evento'] = df['evento'].replace({'adu': 'Adulti', 'bam': 'Bambini', 'ven': 'Venerdì'})

            # Sum presenze by anno_sociale and evento
            df_grouped = df.groupby(['anno_sociale', 'evento'])['presenze'].sum().reset_index()
            
            # Sort chronologically by anno_sociale
            df_grouped = df_grouped.sort_values('anno_sociale')
            
        # 4. Visualization (Executes after spinner)
        # Consistent colors
        color_map = {
            'Adulti': '#198754', 
            'Venerdì': '#dc3545', 
            'Bambini': '#fd7e14'
        }
        
        fig = px.bar(
            df_grouped,
            x="anno_sociale",
            y="presenze",
            color="evento",
            color_discrete_map=color_map,
            barmode='stack',
            title="Andamento Presenze per Anno Sociale",
            labels={"anno_sociale": "Anno Sociale", "presenze": "Totale Presenze", "evento": "Tipo Evento"}
        )
        
        # Improve Layout
        fig.update_layout(
            xaxis_title="Anno Sociale",
            yaxis_title="Presenze Totali",
            legend_title="Tipo Evento"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # --- SECOND CHART: Summer Season (July 1 - Sept 10) ---
        st.divider()
        
        if not df_summer.empty and not df_summer_grouped.empty:
            fig_summer = px.bar(
                df_summer_grouped,
                x="x_label",
                y="presenze",
                text="bar_text",
                title="Cinema al Castello",
                labels={"x_label": "Anno / Costo Biglietto", "presenze": "Totale Presenze"}
            )
            
            fig_summer.update_traces(textposition='auto')
            
            fig_summer.update_layout(
                xaxis_title="Anno",
                yaxis_title="Totale Presenze",
                showlegend=False
            )
            
            st.plotly_chart(fig_summer, use_container_width=True)
        else:
            st.info("Nessun dato trovato per il periodo estivo.")

    except Exception as e:
        st.error(f"Errore durante il recupero dei dati: {e}")

# --- GESTIONE TURNI HELPER FUNCTIONS ---

def get_volontari():
    """Fetch all volunteers from Supabase."""
    if not supabase: return []
    try:
        response = supabase.table("volontari").select("*").order("cognome, nome").execute()
        return response.data if response.data else []
    except Exception as e:
        st.error(f"Errore recupero volontari: {e}")
        return []

def get_turni():
    """Fetch all shifts from Supabase."""
    if not supabase: return []
    try:
        response = supabase.table("turni").select("*").order("data, ora_inizio").execute()
        return response.data if response.data else []
    except Exception as e:
        st.error(f"Errore recupero turni: {e}")
        return []

# --- TURNAZIONI HELPERS ---
def get_turnazioni():
    """Fetch all turnazioni (periods)."""
    if not supabase: return []
    try:
        response = supabase.table("turnazioni").select("*").order("data_inizio").execute()
        return response.data if response.data else []
    except Exception as e:
        # st.error(f"Errore recupero turnazioni: {e}")
        return []

def add_turnazione(nome, start, end):
    """Add a new turnazione."""
    if not supabase: return False
    try:
        data = {
            "nome": nome,
            "data_inizio": start.strftime("%Y-%m-%d"),
            "data_fine": end.strftime("%Y-%m-%d")
        }
        supabase.table("turnazioni").insert(data).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiunta turnazione: {e}")
        return False

def update_turnazione_name(id, new_name):
    if not supabase: return False
    try:
        supabase.table("turnazioni").update({"nome": new_name}).eq("id", id).execute()
        return True
    except Exception as e:
        st.error(f"Errore update turnazione: {e}")
        return False

def delete_turno(id):
    if not supabase: return False
    try:
        supabase.table("turni").delete().eq("id", id).execute()
        return True
    except Exception as e:
        st.error(f"Errore eliminazione turno: {e}")
        return False

def update_turno_limit(id, limit):
    if not supabase: return False
    try:
        supabase.table("turni").update({"max_volontari": limit}).eq("id", id).execute()
        return True
    except Exception as e:
        st.error(f"Errore update limite turno: {e}")
        return False

# --- EXISTING HELPERS UPDATED ---

def add_volontario(nome, cognome, ruoli):
    """Add a new volunteer with duplicate check."""
    if not supabase: return False
    try:
        # Duplicate Check
        existing = supabase.table("volontari").select("id").ilike("nome", nome).ilike("cognome", cognome).execute()
        if existing.data and len(existing.data) > 0:
            st.error("Volontario già presente in archivio.")
            return False
            
        data = {
            "nome": nome,
            "cognome": cognome,
            "ruoli": ruoli 
        }
        supabase.table("volontari").insert(data).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiunta volontario: {e}")
        return False

def delete_volontario(vol_id):
    """Delete a volunteer, handling FK constraints."""
    if not supabase: return False, "DB non connesso"
    try:
        supabase.table("volontari").delete().eq("id", vol_id).execute()
        return True, None
    except Exception as e:
        err_msg = str(e)
        if "foreign key constraint" in err_msg.lower() or "violates foreign key" in err_msg.lower():
            return False, "Impossibile eliminare: il volontario è assegnato a dei turni."
        return False, f"Errore eliminazione: {err_msg}"



def update_volontario_roles(vol_id, new_roles):
    """Update roles for a volunteer."""
    if not supabase: return False
    try:
        supabase.table("volontari").update({"ruoli": new_roles}).eq("id", vol_id).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiornamento ruoli: {e}")
        return False


def update_turnazione_dates(id, new_start, new_end):
    """Update dates for a period with overlap check."""
    if not supabase: return False, "DB Error"
    
    # Check for overlaps
    # Fetch all other periods
    others = supabase.table("turnazioni").select("*").neq("id", id).execute()
    data_list = others.data or []
    
    ns = new_start.strftime("%Y-%m-%d")
    ne = new_end.strftime("%Y-%m-%d")
    
    for p in data_list:
        os = p['data_inizio']
        oe = p['data_fine']
        # Overlap: max(start1, start2) <= min(end1, end2)
        # Note: string comparison works for ISO dates
        if max(ns, os) <= min(ne, oe):
            return False, f"Sovrapposizione con '{p['nome']}'"

    try:
        supabase.table("turnazioni").update({
            "data_inizio": ns,
            "data_fine": ne
        }).eq("id", id).execute()
        return True, "Aggiornato"
    except Exception as e:
        return False, str(e)

    except Exception as e:
        return False, str(e)

def delete_turnazione(id):
    """Delete a period if it has no associated shifts."""
    if not supabase: return False, "DB Error"
    try:
        # Check integrity
        # Count shifts
        shifts = supabase.table("turni").select("id", count="exact").eq("turnazione_id", id).execute()
        # count is usually in shifts.count if count param is used, but Supabase-py might vary.
        # Safer: checks data length if we select id
        if shifts.data and len(shifts.data) > 0:
             return False, "Impossibile eliminare: ci sono turni associati."
        
        supabase.table("turnazioni").delete().eq("id", id).execute()
        return True, "Eliminato"
    except Exception as e:
        return False, str(e)

def find_turnazione_for_date(date_obj):
    """Find active turnazione for a given date."""
    if not supabase: return None, None
    d_str = date_obj.strftime("%Y-%m-%d")
    try:
        # lte = less than or equal (start <= date)
        # gte = greater than or equal (end >= date)
        # Supabase filter: data_inizio <= date AND data_fine >= date
        res = supabase.table("turnazioni").select("id, nome")\
            .lte("data_inizio", d_str)\
            .gte("data_fine", d_str)\
            .execute()
        if res.data and len(res.data) > 0:
            return res.data[0]['id'], res.data[0]['nome']
        return None, None
    except Exception:
        return None, None

def add_turno(data_obj, ora_str, max_volontari=2, turnazione_id=None):
    """Add a new shift."""
    if not supabase: return False, "DB non connesso"
    try:
        # Check duplicate
        d_str = data_obj.strftime("%Y-%m-%d")
        existing = supabase.table("turni").select("id").eq("data", d_str).eq("ora_inizio", ora_str).execute()
        if existing.data and len(existing.data) > 0:
            return False, "Esiste già un turno pianificato per questa data e orario."
            
        payload = {
            "data": d_str,
            "ora_inizio": ora_str,
            "volontari_ids": [],
            "max_volontari": max_volontari
        }
        if turnazione_id:
            payload["turnazione_id"] = turnazione_id
            
        supabase.table("turni").insert(payload).execute()
        return True, "Turno creato correttamente."
    except Exception as e:
        return False, f"Errore aggiunta turno: {e}"

def send_brevo_campaign(message_text, campaign_name):
    """
    Send an SMS campaign via Brevo.
    """
    brevo_config = st.secrets.get("brevo", {})
    api_key = brevo_config.get("api_key")
    sender = brevo_config.get("sms_sender")
    list_id = brevo_config.get("sms_list_id")

    if not api_key:
        return False, "API Key Brevo non configurata."
    if not list_id:
        return False, "ID Lista non configurato."
    if not sender:
        return False, "Mittente SMS non configurato."

    url = "https://api.brevo.com/v3/smsCampaigns"
    
    # Schedule for NOW (UTC)
    scheduled_at = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')

    payload = {
        "name": campaign_name,
        "sender": sender,
        "content": message_text,
        "recipients": {"listIds": [int(list_id)]},
        "scheduledAt": scheduled_at
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
             return True, "Campagna inviata correttamente"
        else:
             return False, f"Errore Brevo: {response.text}"
    except Exception as e:
        return False, f"Errore di connessione: {e}"

def update_turno_staff(turno_id, field, value):
    """
    Update a specific staff field for a shift.
    field: 'responsabile_id', 'tecnico_id', or 'volontari_ids'
    """
    if not supabase: return False
    try:
        supabase.table("turni").update({field: value}).eq("id", turno_id).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiornamento turno ({field}): {e}")
        return False

# Funzione per la pagina Gestione Turni
def render_turni_page():
    st.title("🗓️ Gestione Turni")
    
    global supabase
    if not supabase:
        st.error("Client Supabase non inizializzato.")
        return

    # --- TABS LAYOUT ---
    tab_vol, tab_periodi, tab_turni_list, tab_comms = st.tabs(["👥 Volontari", "📂 Turnazioni", "🗓️ Turni", "📢 Comunicazioni"])
    
    # --- FETCH DATA (GLOBAL SCOPE) ---
    volontari_all = get_volontari()
    # Sort globally for consistency in dropdowns
    volontari_all.sort(key=lambda x: (x.get('cognome', '').lower(), x.get('nome', '').lower()))
    
    turni_all = get_turni()
    
    # Calculate Stats (on full list)
    shift_counts = {}
    for t in turni_all:
         for role in ['responsabile_id', 'tecnico_id']:
             if t.get(role): shift_counts[t[role]] = shift_counts.get(t[role], 0) + 1
         for v_id in t.get('volontari_ids', []) or []:
             shift_counts[v_id] = shift_counts.get(v_id, 0) + 1

    # --- TAB 1: VOLONTARI ---
    with tab_vol:
        col_grid, col_stats = st.columns([4, 1])
        
        # --- LEFT: GRID ---
        with col_grid:
            c_btn, c_search = st.columns([1, 3])
            
            with c_btn:
                with st.popover("➕ Nuovo Volontario", use_container_width=True):
                    st.markdown("#### Aggiungi Volontario")
                    with st.form("form_new_vol"):
                        v_nome = st.text_input("Nome")
                        v_cognome = st.text_input("Cognome")
                        v_ruoli = st.multiselect("Ruoli", ["Responsabile", "Tecnico", "Volontario"], default=["Volontario"])
                        submitted_vol = st.form_submit_button("Salva")
                        if submitted_vol:
                            if v_nome and v_cognome:
                                if add_volontario(v_nome, v_cognome, v_ruoli):
                                    st.success(f"Aggiunto {v_nome} {v_cognome}")
                                    st.rerun()
                            else:
                                st.error("Nome e Cognome obbligatori")

            with c_search:
                search_query = st.text_input("Cerca", placeholder="🔍 Cerca per nome o cognome...", label_visibility="collapsed")
            
            # Data Preparation for Roles
            unique_roles = sorted(list(set([r for v in volontari_all for r in v.get('ruoli', [])])))
            filter_roles = st.multiselect("Filtra per Ruolo", options=unique_roles, placeholder="Seleziona ruoli per filtrare...")

            # Combined Filtering Logic (AND)
            volontari_display = []
            q = search_query.lower() if search_query else ""
            
            for v in volontari_all:
                # 1. Search Match
                match_search = True
                if q:
                    if q not in v.get('nome', '').lower() and q not in v.get('cognome', '').lower():
                        match_search = False
                
                # 2. Role Match
                match_role = True
                if filter_roles:
                    v_roles = set(v.get('ruoli', []))
                    if not v_roles.intersection(set(filter_roles)):
                        match_role = False
                
                if match_search and match_role:
                    volontari_display.append(v)

            st.divider()
            
            # Grid Loop
            cols = st.columns(3)
            for i, v in enumerate(volontari_display):
                v_id = v['id']
                nome = f"{v.get('nome')} {v.get('cognome')}"
                roles = v.get('ruoli', [])
                count = shift_counts.get(v_id, 0)
                
                # Badges
                badges = []
                if "Responsabile" in roles: badges.append("resp")
                if "Tecnico" in roles: badges.append("tec")
                if "Volontario" in roles: badges.append("vol")
                badges_str = " ".join([f"[{b}]" for b in badges]) 
                
                with cols[i % 3]:
                    with st.container(border=True):
                        c1, c2 = st.columns([3, 1])
                        with c1:
                            st.markdown(f"**{nome}**")
                            st.caption(f"{badges_str} - 🎯 {count}")
                        with c2:
                            # Actions
                            # Edit
                            with st.popover("✏️"):
                                new_roles = st.multiselect("Ruoli", ["Responsabile", "Tecnico", "Volontario"], default=roles, key=f"er_{v_id}")
                                if st.button("Salva", key=f"sr_{v_id}"):
                                    update_volontario_roles(v_id, new_roles)
                                    st.rerun()
                            # Delete
                            with st.popover("🗑️"):
                                st.write("Eliminare?")
                                if st.button("Sì", key=f"dr_{v_id}", type="primary"):
                                    ok, msg = delete_volontario(v_id)
                                    if ok: 
                                        st.success("Eliminato correttamente")
                                        time.sleep(0.5)
                                        st.rerun()
                                    else: st.error(msg)
        
        # --- RIGHT: STATS ---
        with col_stats:
            st.markdown("##### 🏆 Top")
            # Sort by count desc (using full list)
            sorted_by_count = sorted(volontari_all, key=lambda x: shift_counts.get(x['id'], 0), reverse=True)
            top_10 = sorted_by_count[:15]
            
            for v in top_10:
                c = shift_counts.get(v['id'], 0)
                if c > 0:
                    st.write(f"**{c}** - {v.get('nome')} {v.get('cognome')[0]}.")
    
    # --- TAB 2: TURNAZIONI ---
    with tab_periodi:
        col_new, col_list = st.columns([1, 2])
        
        with col_new:
            st.markdown("#### Nuova Turnazione")
            with st.form("new_tn_form"):
                tn_nome = st.text_input("Nome", placeholder="Es. Estate 2025")
                tn_start = st.date_input("Inizio", value=datetime.today(), format="DD/MM/YYYY")
                tn_end = st.date_input("Fine", value=datetime.today() + timedelta(days=60), format="DD/MM/YYYY")
                if st.form_submit_button("Crea"):
                    if tn_nome:
                        add_turnazione(tn_nome, tn_start, tn_end)
                        st.success("OK")
                        st.rerun()
                    else: st.error("Nome mancante")
        
        with col_list:
            st.markdown("#### Elenco Periodi")
            periodi = get_turnazioni()
            for p in periodi:
                with st.expander(f"{p['nome']} ({pd.to_datetime(p['data_inizio']).strftime('%d/%m/%Y')} - {pd.to_datetime(p['data_fine']).strftime('%d/%m/%Y')})"):
                    # Edit Name
                    new_n = st.text_input("Nome", value=p['nome'], key=f"ed_tn_{p['id']}")
                    if st.button("Aggiorna Nome", key=f"up_tn_{p['id']}"):
                        update_turnazione_name(p['id'], new_n)
                        st.rerun()
                    
                    st.divider()
                    
                    # Edit Dates
                    c_s, c_e = st.columns(2)
                    d_s = c_s.date_input("Inizio", value=pd.to_datetime(p['data_inizio']), format="DD/MM/YYYY", key=f"ds_{p['id']}")
                    d_e = c_e.date_input("Fine", value=pd.to_datetime(p['data_fine']), format="DD/MM/YYYY", key=f"de_{p['id']}")
                    
                    if st.button("Salva Date", key=f"sv_d_{p['id']}"):
                        ok, msg = update_turnazione_dates(p['id'], d_s, d_e)
                        if ok:
                            st.success(msg)
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error(msg)
                    
                    # Show associated shifts count
                    associated = [t for t in turni_all if t.get('turnazione_id') == p['id']]
                    st.info(f"Turni associati: {len(associated)}")
                    if associated:
                        df_assoc = pd.DataFrame(associated)
                        # Format date in dataframe for display if needed
                        # st.dataframe(df_assoc[['data', 'ora_inizio', 'max_volontari']], hide_index=True)
                        st.write(f"Ultimo turno: {get_latest_date()}") # Just a placeholder or summary
                    
                    # DELETE ZONE
                    st.divider()
                    with st.popover("🗑️ Elimina Periodo", help="Cancellazione sicura"):
                        st.markdown(f"Eliminare **{p['nome']}**?")
                        if st.button("Conferma", type="primary", key=f"del_tn_{p['id']}"):
                             ok, msg = delete_turnazione(p['id'])
                             if ok:
                                 st.success(msg)
                                 time.sleep(1)
                                 st.rerun()
                             else:
                                 st.error(msg)

    # --- TAB 3: TURNI ---
    with tab_turni_list:
        c_add, c_view = st.columns([1, 3])
        
        with c_add:
            st.markdown("#### ➕ Nuovo Turno")
            # Form outside 'form' to allow dynamic validation? No, usually form is better.
            # But we need to validate date -> turnazione mapping.
            
            d_data = st.date_input("Data Turno", value=datetime.today(), format="DD/MM/YYYY")
            d_ora = st.time_input("Ora Inizio", value=pd.Timestamp("21:00").time())
            d_max = st.number_input("Max Vol", 1, 10, 2)
            
            # Validation
            tid, tname = find_turnazione_for_date(d_data)
            
            if tid:
                st.success(f"Associato a: **{tname}**")
                if st.button("Crea Turno", type="primary"):
                    success, msg = add_turno(d_data, d_ora.strftime("%H:%M"), d_max, tid)
                    if success:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
            else:
                st.error("⚠️ Nessuna Turnazione attiva in questa data. Crea prima il periodo.")
                st.button("Crea Turno", disabled=True)

        with c_view:
            st.markdown("#### Calendario Turni")
            
            # Filter Logic
            # show filters?
            # List all sort by date
            
            # Helper Mappings (Using volontari_all)
            candidate_resp = [v for v in volontari_all if "Responsabile" in v.get('ruoli', [])]
            candidate_tec = [v for v in volontari_all if "Tecnico" in v.get('ruoli', [])]
            candidate_vol = volontari_all
            # Maps
            def get_n(v): return f"{v.get('nome')} {v.get('cognome')}"
            map_r = {v['id']: get_n(v) for v in candidate_resp}
            map_t = {v['id']: get_n(v) for v in candidate_tec}
            map_v = {v['id']: get_n(v) for v in candidate_vol}

            for i, turno in enumerate(turni_all): # Use enumerate for safety if needed
                t_id = turno['id']
                t_date = pd.to_datetime(turno['data'])
                header = f"{t_date.strftime('%d/%m/%Y')} - {turno['ora_inizio'][:5]}"
                
                with st.container(border=True):
                    # Header + Actions
                    h1, h2, h3 = st.columns([5, 1, 1])
                    h1.markdown(f"**{header}**")
                    with h2:
                         with st.popover("⚙️"):
                            nm = st.number_input("Max", 1, 10, turno.get('max_volontari', 2), key=f"mx_{t_id}")
                            if nm != turno.get('max_volontari', 2):
                                update_turno_limit(t_id, nm)
                                st.rerun()
                    with h3:
                        with st.popover("🗑️", help="Elimina Turno"):
                            st.write("Confermi l'eliminazione del turno?")
                            if st.button("Sì, Elimina", type="primary", key=f"confirm_del_turno_{t_id}"):
                                delete_turno(t_id)
                                st.success("Turno eliminato")
                                st.rerun()
                    
                    # Slots
                    s1, s2, s3 = st.columns(3)
                    
                    # 1. Responsabile (CLEANED)
                    curr_r = turno.get('responsabile_id')
                    # Generate a unique key using 's_r_' prefix
                    sel_r = s1.selectbox(
                        "🟠 Resp", 
                        [None] + list(map_r.keys()), 
                        format_func=lambda x: map_r[x] if x else "-", 
                        key=f"s_r_{t_id}", 
                        index=list(map_r.keys()).index(curr_r)+1 if curr_r in map_r else 0
                    )
                    if sel_r != curr_r:
                        update_turno_staff(t_id, "responsabile_id", sel_r)
                        st.rerun()

                    # 2. Tecnico (CLEANED)
                    curr_t = turno.get('tecnico_id')
                    sel_t = s2.selectbox(
                        "🟢 Tec", 
                        [None] + list(map_t.keys()), 
                        format_func=lambda x: map_t[x] if x else "-", 
                        key=f"s_t_{t_id}", 
                        index=list(map_t.keys()).index(curr_t)+1 if curr_t in map_t else 0
                    )
                    if sel_t != curr_t:
                        update_turno_staff(t_id, "tecnico_id", sel_t)
                        st.rerun()

                    # 3. Volontari (CLEANED)
                    row_max = turno.get('max_volontari', 2)
                    curr_v = turno.get('volontari_ids') or []
                    # Filter current volunteers to ensure they exist in map_v to avoid errors
                    valid_curr_v = [x for x in curr_v if x in map_v]
                    
                    sel_v = s3.multiselect(
                        f"🔵 Vol (Max {row_max})", 
                        list(map_v.keys()), 
                        default=valid_curr_v, 
                        format_func=lambda x: map_v[x], 
                        key=f"s_v_{t_id}", 
                        max_selections=row_max
                    )
                    
                    # Compare sets to detect changes (ignoring order)
                    if set(sel_v) != set(curr_v):
                         update_turno_staff(t_id, "volontari_ids", sel_v)
                         st.rerun()

    # --- TAB 4: COMUNICAZIONI ---
    with tab_comms:
        st.markdown("#### 📢 Comunicazioni ai Volontari")
        st.info("Invia SMS massivi alle liste Brevo configurate.")
        
        # Sort periods by start date
        periodi_sorted = sorted(get_turnazioni(), key=lambda x: x['data_inizio'])
        brevo_list_id = st.secrets.get("brevo", {}).get("sms_list_id", "N/A")
        
        for p in periodi_sorted:
            start_fmt = pd.to_datetime(p['data_inizio']).strftime('%d/%m/%Y')
            end_fmt = pd.to_datetime(p['data_fine']).strftime('%d/%m/%Y')
            
            with st.container(border=True):
                c_info, c_act = st.columns([4, 1])
                with c_info:
                    st.markdown(f"**{p['nome']}** | Dal {start_fmt} Al {end_fmt}")
                with c_act:
                    with st.popover("✉️ Invia SMS", help="Invia messaggio alla lista Brevo"):
                        st.markdown("##### Prepara Messaggio")
                        default_msg = f"Ciao! La registrazione per i turni dal {start_fmt} al {end_fmt} è aperta. Accedi all'app per dare la tua disponibilità."
                        msg_body = st.text_area("Testo Messaggio", value=default_msg, height=150, max_chars=160, key=f"msg_{p['id']}")
                        
                        st.caption(f"Invio alla Lista Brevo ID: **{brevo_list_id}**")
                        
                        if st.button(f"🚀 INVIA ORA", type="primary", key=f"snd_{p['id']}"):
                            ok, res = send_brevo_campaign(msg_body, f"Avviso {p['nome']}")
                            if ok:
                                st.success(res)
                            else:
                                st.error(res)

# Funzione per la pagina Proiezioni
def render_proiezioni_page():
    st.title("📽️ Report & Proiezioni")

    global supabase
    if not supabase:
        st.error("Client Supabase non inizializzato.")
        return

    # --- 1. FETCH ALL DATA ---
    try:
        # Helper for Pagination (Bypass 1000 limit)
        def fetch_all_highlights():
            all_data = []
            start = 0
            batch_size = 1000
            while True:
                response = supabase.table("eventi_highlights").select('*').range(start, start + batch_size - 1).execute()
                rows = response.data
                if not rows:
                    break
                all_data.extend(rows)
                
                if len(rows) < batch_size:
                    break
                start += batch_size
            return all_data

        data_rows = fetch_all_highlights()
        
        if not data_rows:
            st.info("Nessun dato disponibile nel report (eventi_highlights vuoto).")
            return
            
        df = pd.DataFrame(data_rows)
        
        # Ensure Numeric Types
        df['ingressi'] = pd.to_numeric(df['ingressi'], errors='coerce').fillna(0).astype(int)
        df['incasso'] = pd.to_numeric(df['incasso'], errors='coerce').fillna(0.0)
        
        # Ensure Date Type
        df['data'] = pd.to_datetime(df['data'])

    except Exception as e:
        st.error(f"Errore nel recupero dati: {e}")
        return

    # --- HELPER FUNCTIONS ---
    def fmt_money(val):
        return f"€ {val:,.0f}".replace(',', '.')
    
    def fmt_num(val):
        return f"{int(val):,}".replace(',', '.')

    def fmt_date_extended(d):
        try:
            days_map = {0: 'lunedì', 1: 'martedì', 2: 'mercoledì', 3: 'giovedì', 4: 'venerdì', 5: 'sabato', 6: 'domenica'}
            return f"{days_map[d.weekday()]} {d.strftime('%d/%m/%Y')}"
        except: return str(d)

    # --- 2. REPORTS SECTION (Vertical Layout) ---
    st.subheader("📊 Classifiche Stagionali")
    
    tab1, tab2, tab3 = st.tabs(["🏆 Classifica Generale", "🔝 Top 10 (Singolo)", "📉 Flop 10 (Singolo)"])
    
    # TAB 1: CLASSIFICA GENERALE (Aggregated)
    with tab1:
        st.caption("Classifica basata sulla somma degli ingressi di tutte le proiezioni.")
        
        df_agg = df.groupby(['titolo_evento', 'autore', 'nazione']).agg({
            'ingressi': 'sum',
            'incasso': 'sum',
            'data': 'min',
            'titolo_evento': 'count' # Trick to get count, will be renamed
        }).rename(columns={'titolo_evento': 'proiezioni_count'}).reset_index()
        
        # Sort & Top 10
        df_rank = df_agg.sort_values(by='ingressi', ascending=False).head(10).copy()
        
        # Formatting
        df_rank['Data*'] = df_rank['data'].apply(fmt_date_extended)
        df_rank['Ingressi (N. Proiez.)'] = df_rank.apply(
            lambda x: f"{fmt_num(x['ingressi'])} ({x['proiezioni_count']})", axis=1
        )
        df_rank['Incasso Totale'] = df_rank['incasso'].apply(fmt_money)
        
        st.dataframe(
            df_rank[['titolo_evento', 'autore', 'Ingressi (N. Proiez.)', 'Incasso Totale', 'Data*', 'nazione']].rename(
                columns={'titolo_evento': 'Film', 'autore': 'Autore', 'nazione': 'Naz.'}
            ),
            hide_index=True,
            use_container_width=True
        )
        st.caption("(*) Si riferisce alla data della prima proiezione.")

    # TAB 2: TOP 10 (Single Event)
    with tab2:
        st.caption("I 10 singoli eventi con più spettatori.")
        df_top = df.sort_values(by='ingressi', ascending=False).head(10).copy()
        
        df_top['Incasso'] = df_top['incasso'].apply(fmt_money)
        df_top['Ingressi'] = df_top['ingressi'].apply(fmt_num)
        df_top['Data'] = df_top['data'].apply(fmt_date_extended)
        
        st.dataframe(
            df_top[['Data', 'titolo_evento', 'Ingressi', 'Incasso']].rename(
                columns={'titolo_evento': 'Film'}
            ),
            hide_index=True,
            use_container_width=True
        )

    # TAB 3: FLOP 10 (Single Event)
    with tab3:
        st.caption("I 10 singoli eventi con meno spettatori.")
        df_flop = df.sort_values(by='ingressi', ascending=True).head(10).copy()
        
        df_flop['Incasso'] = df_flop['incasso'].apply(fmt_money)
        df_flop['Ingressi'] = df_flop['ingressi'].apply(fmt_num)
        df_flop['Data'] = df_flop['data'].apply(fmt_date_extended)
        
        st.dataframe(
            df_flop[['Data', 'titolo_evento', 'Ingressi', 'Incasso']].rename(
                columns={'titolo_evento': 'Film'}
            ),
            hide_index=True,
            use_container_width=True
        )

    # --- 3. SEARCH SECTION (Vertical Layout - Below) ---
    st.divider()
    st.subheader("🔍 Cerca Film")
    
    # Unique titles from the DF
    titles = sorted(df['titolo_evento'].unique())
    
    col_sel, col_empty = st.columns([1, 2]) # Limit selectbox width
    with col_sel:
        selected_movie = st.selectbox(
            "Seleziona un titolo",
            options=titles,
            index=None,
            placeholder="Scrivi per cercare...",
            label_visibility="collapsed"
        )
    
    if selected_movie:
        # Filter Data
        movie_data = df[df['titolo_evento'] == selected_movie]
        
        # Aggregates for Card
        tot_ing = movie_data['ingressi'].sum()
        tot_inc = movie_data['incasso'].sum()
        n_proj = len(movie_data)
        dates = sorted(movie_data['data'].unique())
        dates_str = ", ".join([d.strftime('%d/%m') for d in dates])
        
        fmt_inc = fmt_money(tot_inc)
        fmt_ing = fmt_num(tot_ing)
        
        st.markdown(f"""
        <div style="background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-top: 5px solid #007bff; margin-bottom: 20px;">
            <h3 style="margin-top:0; color: #333;">{selected_movie}</h3>
            <p style="color: #666; font-size: 0.9em;">{dates_str}</p>
            <hr style="margin: 10px 0;">
            <div style="display:flex; justify-content:space-between; margin-bottom: 5px;">
                <span>📽️ Proiezioni:</span>
                <strong>{n_proj}</strong>
            </div>
            <div style="display:flex; justify-content:space-between; margin-bottom: 5px;">
                <span>👥 Ingressi:</span>
                <strong>{fmt_ing}</strong>
            </div>
            <div style="display:flex; justify-content:space-between;">
                <span>💰 Incasso:</span>
                <strong>{fmt_inc}</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Mini Table of Screenings
        st.markdown("**Dettaglio Proiezioni:**")
        
        mini_df = movie_data[['data', 'ingressi', 'incasso']].copy()
        mini_df['data'] = mini_df['data'].apply(fmt_date_extended)
        mini_df['incasso'] = mini_df['incasso'].apply(fmt_money)
        mini_df['ingressi'] = mini_df['ingressi'].apply(fmt_num)
        
        st.dataframe(
            mini_df.rename(columns={'data': 'Data', 'ingressi': 'Ingr.', 'incasso': 'Inc.'}),
            hide_index=True,
            use_container_width=True
        )


# Main App Navigation
def main():
    # 1. CSS for Cards and Sidebar visibility
    st.markdown("""
<style>
    /* 1. Make Header visible but transparent so we can click the "Expand" arrow */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    
    /* 2. Hide only the decorative colored bar at the top */
    div[data-testid="stDecoration"] {
        display: none;
    }
    
    /* 2. Target the Radio Button in the Sidebar specifically */
    section[data-testid="stSidebar"] div[role="radiogroup"] {
        background-color: transparent;
    }

    /* 3. HIDE the Radio Circle (The input bubble) */
    section[data-testid="stSidebar"] div[role="radiogroup"] label > div:first-child {
        display: none !important;
    }

    /* 4. Style the Labels (The menu items) to look like Buttons */
    section[data-testid="stSidebar"] div[role="radiogroup"] label {
        padding: 12px 20px !important;
        border-radius: 8px !important;
        margin-bottom: 4px !important;
        border: none !important;
        transition: all 0.2s ease;
        cursor: pointer;
        font-size: 16px !important;
        color: #444 !important; /* Default text color */
    }

    /* 5. Hover Effect */
    section[data-testid="stSidebar"] div[role="radiogroup"] label:hover {
        background-color: #f0f2f6 !important; /* Light Gray on hover */
        color: #000 !important;
    }

    /* 6. ACTIVE State (The selected page) - Brevo Style */
    section[data-testid="stSidebar"] div[role="radiogroup"] label[data-checked="true"] {
        background-color: #e6f3ff !important; /* Light Blue Background */
        color: #007bff !important; /* Blue Text */
        font-weight: 600 !important;
        border-left: 4px solid #007bff !important; /* Accent bar on left */
    }
    
    /* 7. Card Styling (Keep existing) */
    div[data-testid="stMetric"], div[data-testid="stVerticalBlock"] > div[style*="background-color: white"] {
        background-color: white;
        border-radius: 12px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
</style>
    """, unsafe_allow_html=True)

    # --- 1. AUTHENTICATION GATEKEEPER ---
    ms_token = st.session_state.get("ms_token")
    
    # MSAL Configuration
    ms_config = st.secrets.get("microsoft", {})
    CLIENT_ID = ms_config.get("client_id")
    CLIENT_SECRET = ms_config.get("client_secret")
    TENANT_ID = ms_config.get("tenant_id")
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    REDIRECT_URI = "http://localhost:8501" 
    SCOPE = ["User.Read", "User.ReadBasic.All"]

    # Handle Callback Logic (Code Exchange) - Runs even if no token yet
    if 'code' in st.query_params:
         code = st.query_params['code']
         try:
             app = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
             result = app.acquire_token_by_authorization_code(code, scopes=SCOPE, redirect_uri=REDIRECT_URI)
             if "error" in result:
                 st.error(result.get("error_description"))
             else:
                 st.session_state["ms_user"] = result.get("id_token_claims")
                 st.session_state["ms_token"] = result.get("access_token")
                 st.query_params.clear()
                 st.rerun()
         except Exception as e:
             st.error(f"Errore Login: {e}")
             return

    if not st.session_state.get("ms_token"):
        # RENDER LOGIN PAGE (Splash Screen)
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.write("") # Spacer
            st.write("")
            logo_path = "assets/Logo_Metropol.png"
            if os.path.exists(logo_path):
                st.image(logo_path, width=200)
            st.title("Dashboard Eventi")
            
            # Login Button
            if CLIENT_ID:
                 app = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
                 auth_url = app.get_authorization_request_url(SCOPE, redirect_uri=REDIRECT_URI)
                 # HTML Link disguised as a Button
                 # target="_blank" is the only robust way to handle OAuth on Streamlit Cloud
                 st.markdown(f'''
                    <a href="{auth_url}" target="_blank" style="
                        display: inline-block;
                        padding: 0.5rem 1rem;
                        color: white;
                        background-color: #ff4b4b;
                        border-radius: 0.5rem;
                        text-decoration: none;
                        font-weight: 500;
                        text-align: center;
                        width: 100%;
                        font-family: 'Source Sans Pro', sans-serif;">
                        🔑 Accedi con account Office 365 (Nuova Scheda)
                    </a>
                 ''', unsafe_allow_html=True)
            else:
                st.error("Configurazione Microsoft incomplete.")
        return # STOP EXECUTION

    # --- 2. AUTHORIZATION (RBAC) ---
    # User is Logged In
    user_email = st.session_state.get("ms_user", {}).get("mail") or st.session_state.get("ms_user", {}).get("preferred_username")
    user_name = st.session_state.get("ms_user", {}).get("name")
    
    user_role = get_user_role(user_email)
    
    if not user_role:
        st.error("⛔ Accesso Negato: Utente non autorizzato.")
        if st.button("Logout"):
            del st.session_state["ms_token"]
            if "ms_user" in st.session_state: del st.session_state["ms_user"]
            st.rerun()
        return # STOP EXECUTION

    is_admin = (user_role == "Amministratore")

    # --- 3. RENDER APP (AUTHORIZED) ---

    # SIDEBAR
    with st.sidebar:
        # Logo
        logo_path = "assets/Logo_Metropol.png"
        if os.path.exists(logo_path):
            st.image(logo_path, use_container_width=True)
        
        st.markdown("### Dashboard")
        st.markdown("---")
        
        # Navigation
        selected_page = st.radio(
            "Menu",
            options=["📊 Statistiche", "📑 Riepiloghi", "📽️ Proiezioni", "🗓️ Gestione Turni"],
            label_visibility="collapsed"
        )

    # SETTINGS STATE
    if "settings_view" not in st.session_state:
        st.session_state.settings_view = None
    
    active_view = selected_page 
    if st.session_state.settings_view:
        active_view = st.session_state.settings_view

    # TOP BAR (Header)
    col_title, col_config = st.columns([5, 2])
    
    with col_title:
        st.markdown(f"## {active_view}")
        
    with col_config:
        # Display User Info & Actions
        c_info, c_act = st.columns([2, 1])
        with c_info:
            st.caption(f"{user_name}\n({user_role})")
        with c_act:
             if st.button("Esci", key="top_logout", type="secondary"):
                 del st.session_state["ms_token"]
                 if "ms_user" in st.session_state: del st.session_state["ms_user"]
                 st.session_state.settings_view = None
                 st.rerun()

        # Gear Icon (Admin Only)
        if is_admin:
             with st.popover("⚙️", help="Impostazioni"):
                st.markdown("**Impostazioni**")
                if st.button("Configurazione Generale", use_container_width=True):
                    st.session_state.settings_view = "Configurazione"
                    st.rerun()
                if st.button("Importa Dati", use_container_width=True):
                    st.session_state.settings_view = "Importa Dati"
                    st.rerun()
                if st.button("Gestione Utenti", use_container_width=True):
                    st.session_state.settings_view = "Utenti"
                    st.rerun()
                
                st.divider()
                if st.button("🔙 Torna alla Dashboard", type="primary", use_container_width=True):
                    st.session_state.settings_view = None
                    st.rerun()

    st.divider()

    # ROUTING (Strict Checks)
    if active_view == "📊 Statistiche":
        render_consulta_page()
        
    elif active_view == "📑 Riepiloghi":
        render_riepiloghi_page()
        
    elif active_view == "📽️ Proiezioni":
        render_proiezioni_page()
        
    elif active_view == "🗓️ Gestione Turni":
        if user_role in ['Amministratore', 'Gestione Turni']:
            render_turni_page()
        else:
             st.error("⛔ Accesso non autorizzato. Contatta l'amministratore.")
        
    # Admin Pages
    elif active_view in ["Configurazione", "Importa Dati", "Utenti"]:
        if is_admin:
            if active_view == "Configurazione": render_config_page()
            elif active_view == "Importa Dati": render_import_page()
            elif active_view == "Utenti": render_users_page()
        else:
            st.error("⛔ Non hai i permessi per visualizzare questa pagina.")
    else:
        st.error(f"Pagina non trovata: {active_view}")

if __name__ == "__main__":
    main()
