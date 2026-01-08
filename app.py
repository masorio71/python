import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import numpy as np
import toml
import os
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

# Funzione per la pagina di Configurazione
def render_config_page():
    st.title("⚙️ Configurazione")
    
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
        
        submitted = st.form_submit_button("Salva Configurazione")
        
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
                    
                    st.success("Configurazione salvata con successo! L'applicazione si aggiornerà.")
                    
                except Exception as e:
                    st.error(f"Errore durante il salvataggio: {e}")

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

            col_indices = [0, 3, 2, 5, 8, 11]
            selected_columns = df_uploaded.iloc[:, col_indices].copy()
            
            original_col_a = selected_columns.columns[0]
            selected_columns = selected_columns.rename(columns={original_col_a: 'Data'})

            if "Incasso Totale Lordo" in selected_columns.columns:
                selected_columns = selected_columns.rename(columns={"Incasso Totale Lordo": "Incasso"})
            
            selected_columns['temp_datetime'] = pd.to_datetime(selected_columns['Data'], dayfirst=True, errors='coerce')
            
            # Use shared get_evento
            selected_columns['Evento'] = selected_columns['temp_datetime'].apply(get_evento)

            if "Incasso" in selected_columns.columns:
                selected_columns["Incasso"] = pd.to_numeric(selected_columns["Incasso"], errors='coerce')
            
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

            event_name_col = selected_columns.columns[1]
            
            def check_vos(val):
                if not isinstance(val, str):
                    return False
                val_lower = val.lower()
                keywords = ["(eng)", "(en)", "(originale)", "(vos)"]
                return any(k in val_lower for k in keywords)

            selected_columns['VOS'] = selected_columns[event_name_col].apply(check_vos)

            # Format Data and Drop temp_datetime ONLY NOW
            selected_columns['Data'] = selected_columns['temp_datetime'].dt.strftime('%Y-%m-%d')
            selected_columns = selected_columns.drop(columns=['temp_datetime'])
            
            cols = ['Data', 'Evento', 'VOS', 'RASSEGNA'] + [c for c in selected_columns.columns if c not in ['Data', 'Evento', 'VOS', 'RASSEGNA']]
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
                        existing_data_response = supabase.table(DB_TABLE_NAME).select("Data", f'"{event_name_col}"').execute()
                        existing_records = existing_data_response.data if existing_data_response.data else []
                        
                        existing_set = set()
                        for record in existing_records:
                            r_date = record.get('Data')
                            r_title = record.get(event_name_col)
                            if r_date and r_title:
                                existing_set.add((r_date, r_title))
                        
                        new_records = []
                        skipped_count = 0
                        
                        for index, row in df_uploaded.iterrows():
                            row_date = row['Data']
                            row_title = row[event_name_col]
                            
                            if (row_date, row_title) in existing_set:
                                skipped_count += 1
                            else:
                                new_records.append(row.to_dict())
                        
                        if new_records:
                            response = supabase.table(DB_TABLE_NAME).insert(new_records).execute()
                            st.success(f"Caricamento completato! Inserite {len(new_records)} nuove righe.")
                            if skipped_count > 0:
                                st.warning(f"Saltate {skipped_count} righe perché già presenti nel database (duplicati Data + Titolo).")
                        else:
                            st.warning(f"Nessuna nuova riga da inserire. Tutte le {skipped_count} righe sono già presenti nel database.")

                    except Exception as e:
                        error_msg = str(e)
                        if "PGRST205" in error_msg or "Could not find the table" in error_msg:
                            st.error(f"Errore: La tabella '{DB_TABLE_NAME}' non esiste in Supabase.")
                            # Removed SQL helper for brevity/mismatch risk, or kept? Keeping as in original to be safe but short version in replacement logic above.
                            st.warning("Supabase non crea automaticamente le tabelle. Controlla che la tabella esista.")
                        else:
                            st.error(f"Errore durante il caricamento: {e}")
                            st.info("Nota: Assicurati che la tabella esista in Supabase e che le colonne corrispondano.")
                        
        except Exception as e:
            st.error(f"Errore nella lettura del file: {e}")
    else:
        st.info("Carica un file Excel (ExportEventi) per iniziare.")

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

    # --- VALORIZZA DB (Generi TMDB) ---
    st.markdown("---")
    st.subheader("Valorizza DB (Generi TMDB)")

    # 1. Schema Check
    if supabase:
        try:
            # Try to select the new columns to see if they exist
            supabase.table(DB_TABLE_NAME).select("genere, tmdb_processed").limit(1).execute()
        except Exception as e:
            if "column" in str(e) and "does not exist" in str(e):
                st.warning("Le colonne 'genere' e 'tmdb_processed' mancano nella tabella.")
                st.code(f"""
                ALTER TABLE public.{DB_TABLE_NAME} 
                ADD COLUMN IF NOT EXISTS genere TEXT,
                ADD COLUMN IF NOT EXISTS tmdb_processed BOOLEAN DEFAULT FALSE;
                """, language="sql")
            # else: ignore other errors or let them surface later

    # 2. Logic Flow
    col_btn, col_limit = st.columns([3, 1])
    
    with col_limit:
        limit_options = [10, 50, 100, "TUTTI"]
        limit_selection = st.selectbox("Righe da elaborare", limit_options, index=1) # Default 50

    with col_btn:
        st.write("") # Spacer to align with selectbox label
        st.write("")
        start_btn = st.button("Valorizza Database", type="primary", use_container_width=True)

    if start_btn:
        if not supabase:
             st.error("Supabase non connesso.")
        else:
            tmdb_api_key = st.secrets.get("tmdb", {}).get("api_key")
            if not tmdb_api_key:
                st.error("API Key TMDB mancante in secrets.toml.")
            else:
                import requests
                
                # Step A: Fetch Genre Mapping
                try:
                    url_genres = f"https://api.themoviedb.org/3/genre/movie/list?api_key={tmdb_api_key}&language=it-IT"
                    resp_genres = requests.get(url_genres)
                    if resp_genres.status_code == 200:
                        genres_data = resp_genres.json().get('genres', [])
                        genre_map = {g['id']: g['name'] for g in genres_data}
                    else:
                        st.error(f"Errore TMDB Genres: {resp_genres.status_code}")
                        genre_map = {}
                except Exception as e:
                    st.error(f"Eccezione TMDB Genres: {e}")
                    genre_map = {}

                if genre_map:
                    # Step B: Select Candidates
                    try:
                        # Determine Limit
                        if limit_selection == "TUTTI":
                            fetch_limit = 1000
                        else:
                            fetch_limit = int(limit_selection)

                        response_candidates = supabase.table(DB_TABLE_NAME)\
                            .select("id, \"Titolo Evento\"")\
                            .or_("tmdb_processed.is.null,tmdb_processed.eq.false")\
                            .limit(fetch_limit)\
                            .execute()
                        
                        candidates = response_candidates.data if response_candidates.data else []
                        
                        if not candidates:
                            st.info("Nessun film da elaborare (tutti già processati).")
                        else:
                            st.info(f"Inizio elaborazione di {len(candidates)} film...")
                            progress_bar = st.progress(0)
                            processed_count = 0
                            found_count = 0
                            
                            for i, row in enumerate(candidates):
                                row_id = row['id']
                                raw_title = row.get('Titolo Evento', '')
                                
                                # Step C: Loop & Process
                                # Clean Title
                                clean_title = raw_title.replace("(VOS)", "").replace("(vos)", "")
                                import re
                                clean_title = re.sub(r'\([^)]*\)', '', clean_title).strip()
                                
                                found_genres_str = None
                                match_found = False
                                
                                if clean_title:
                                    # Search API
                                    search_url = f"https://api.themoviedb.org/3/search/movie?api_key={tmdb_api_key}&query={clean_title}&language=it-IT"
                                    try:
                                        search_resp = requests.get(search_url)
                                        if search_resp.status_code == 200:
                                            results = search_resp.json().get('results', [])
                                            if results:
                                                first_match = results[0]
                                                g_ids = first_match.get('genre_ids', [])
                                                g_names = [genre_map.get(gid) for gid in g_ids if gid in genre_map]
                                                g_names = [gn for gn in g_names if gn] # filter Nones
                                                
                                                if g_names:
                                                    found_genres_str = ", ".join(g_names)
                                                    match_found = True
                                    except Exception as e:
                                        print(f"Error searching {clean_title}: {e}")

                                # Update DB
                                update_data = {"tmdb_processed": True}
                                if match_found:
                                    update_data["genere"] = found_genres_str
                                    found_count += 1
                                
                                supabase.table(DB_TABLE_NAME).update(update_data).eq("id", row_id).execute()
                                processed_count += 1
                                progress_bar.progress((i + 1) / len(candidates))
                            
                            st.success(f"Elaborazione completata! Elaborati {processed_count} film. Generi trovati per {found_count} film.")
                            
                    except Exception as e:
                        st.error(f"Errore durante l'elaborazione: {e}")

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
                    'Data': date.strftime('%Y-%m-%d'),
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
        res = supabase.table(DB_TABLE_NAME).select("Data").order("Data", desc=True).limit(1).execute()
        if res.data and len(res.data) > 0:
            return res.data[0]['Data']
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
                .gte("Data", start_date.strftime('%Y-%m-%d')) \
                .lte("Data", end_date.strftime('%Y-%m-%d')) \
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
                st.session_state.df_main = pd.DataFrame(columns=["Data", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento"])

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
                        .gte("Data", start_date_p2.strftime('%Y-%m-%d')) \
                        .lte("Data", end_date_p2.strftime('%Y-%m-%d')) \
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
                        st.session_state.df_compare = pd.DataFrame(columns=["Data", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento"])

                except Exception as e:
                     st.error(f"Errore recupero dati Periodo 2: {e}")
                     st.session_state.df_compare = pd.DataFrame(columns=["Data", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento"])

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
            st.session_state.df_main = pd.DataFrame(columns=["Data", "Titolo Evento", "Incasso", "Presenze", "Nazionalità", "evento"])
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

            cols_to_drop = ['id', 'Evento', 'VOS', 'temp_datetime']
            display_df = display_df.drop(columns=[c for c in cols_to_drop if c in display_df.columns])

            column_config = {
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
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

# Funzione per la pagina Utenti (Microsoft 365)
def render_users_page():
    st.title("👥 Utenti Microsoft 365")

    # Load MS Config
    ms_config = st.secrets.get("microsoft", {})
    CLIENT_ID = ms_config.get("client_id")
    CLIENT_SECRET = ms_config.get("client_secret")
    TENANT_ID = ms_config.get("tenant_id")
    
    # Constants
    REDIRECT_URI = "http://localhost:8501" 
    SCOPE = ["User.Read", "User.ReadBasic.All"]

    if not CLIENT_ID or not CLIENT_SECRET or not TENANT_ID:
        st.warning("Configurazione Microsoft mancante. Vai alla pagina 'Configurazione' nel menu laterale.")
        return

    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"

    # Initialize MSAL
    try:
        app = msal.ConfidentialClientApplication(
            CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
        )
    except Exception as e:
        st.error(f"Errore inizializzazione MSAL: {e}")
        return

    # Check for Auth Code in Query Params (Callback)
    if 'code' in st.query_params:
        code = st.query_params['code']
        try:
            result = app.acquire_token_by_authorization_code(
                code, scopes=SCOPE, redirect_uri=REDIRECT_URI
            )
            if "error" in result:
                st.error(f"Errore Login: {result.get('error_description')}")
            else:
                st.session_state["ms_user"] = result.get("id_token_claims")
                st.session_state["ms_token"] = result.get("access_token")
                # Clear code from URL
                st.query_params.clear()
                st.rerun()
        except Exception as e:
            st.error(f"Errore durante lo scambio del token: {e}")

    # Check Login State
    if "ms_token" in st.session_state:
        user_name = st.session_state.get('ms_user', {}).get('name', 'Utente')
        
        col_info, col_logout = st.columns([3, 1])
        with col_info:
            st.success(f"Loggato come: **{user_name}**")
        with col_logout:
            if st.button("Logout", type="secondary"):
                del st.session_state["ms_token"]
                if "ms_user" in st.session_state:
                    del st.session_state["ms_user"]
                st.rerun()
            
        # Fetch Users from Graph API
        st.subheader("Elenco Utenti Organizzazione")
        
        headers = {'Authorization': 'Bearer ' + st.session_state['ms_token']}
        graph_url = "https://graph.microsoft.com/v1.0/users?$select=displayName,mail,jobTitle,id"
        
        with st.spinner("Caricamento utenti..."):
            try:
                response = requests.get(graph_url, headers=headers)
                if response.status_code == 200:
                    users = response.json().get('value', [])
                    if users:
                        df_users = pd.DataFrame(users)
                        # Rename columns for better display
                        df_users = df_users.rename(columns={
                            "displayName": "Nome Visualizzato",
                            "mail": "Email",
                            "jobTitle": "Qualifica",
                            "id": "ID Utente"
                        })
                        st.dataframe(df_users, use_container_width=True)
                    else:
                        st.info("Nessun utente trovato.")
                else:
                    st.error(f"Errore API Graph: {response.status_code} - {response.text}")
            except Exception as e:
                st.error(f"Errore richiesta: {e}")

    else:
        # Show Login Button
        st.info("Effettua il login con il tuo account Microsoft 365 per visualizzare gli utenti.")
        auth_url = app.get_authorization_request_url(SCOPE, redirect_uri=REDIRECT_URI)
        
        # Use HTML for a cleaner link/button look that redirects properly
        st.markdown(f'''
            <a href="{auth_url}" target="_self">
                <button style="
                    background-color: #0078D4;
                    color: white;
                    border: none;
                    padding: 10px 20px;
                    border-radius: 4px;
                    cursor: pointer;
                    font-weight: bold;
                    font-family: 'Segoe UI', sans-serif;
                ">
                    Login con Microsoft 365
                </button>
            </a>
        ''', unsafe_allow_html=True)

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
            response = supabase.table(DB_TABLE_NAME).select('Data, "Tot. Presenze", Evento, Incasso').neq('"Tot. Presenze"', 0).not_.is_('"Tot. Presenze"', "null").execute()
            
            if not response.data:
                st.warning("Nessun dato disponibile.")
                return

            df = pd.DataFrame(response.data)
            if not df.empty:
                # Standardize column names for easier processing
                # Map "Data" -> "data" AND "Tot. Presenze" -> "presenze"
                df.rename(columns={
                    'Data': 'data', 
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

def add_volontario(nome, cognome, ruoli):
    """Add a new volunteer."""
    if not supabase: return False
    try:
        data = {
            "nome": nome,
            "cognome": cognome,
            "ruoli": ruoli # expected array/list
        }
        supabase.table("volontari").insert(data).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiunta volontario: {e}")
        return False

def add_turno(data_obj, ora_str):
    """Add a new shift."""
    if not supabase: return False
    try:
        payload = {
            "data": data_obj.strftime("%Y-%m-%d"),
            "ora_inizio": ora_str,
            # responsiabile_id, tecnico_id, volontari_ids fields will be null initially or empty
            "volontari_ids": [] 
        }
        supabase.table("turni").insert(payload).execute()
        return True
    except Exception as e:
        st.error(f"Errore aggiunta turno: {e}")
        return False

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

    # --- 1. Layout Structure ---
    col_sidebar, col_main = st.columns([1, 3])
    
    # --- 2. Sidebar Logic (Pool Volontari) ---
    with col_sidebar:
        st.markdown("### 👥 Pool Volontari")
        
        # Add Button (Popover)
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
                            st.success("Volontario aggiunto!")
                            st.rerun() # Refresh to show new vol
                    else:
                        st.error("Nome e Cognome obbligatori.")

        # Filters
        filter_text = st.text_input("🔍 Cerca...", placeholder="Nome o cognome")
        filter_role = st.multiselect("Mostra solo...", ["Responsabile", "Tecnico", "Volontario"])
        
        # Fetch Data
        volontari = get_volontari()
        turni_all = get_turni()
        
        # Calculate Shift Counts per Volunteer
        # turni has volontari_ids (list), responsabile_id (int), tecnico_id (int)
        shift_counts = {}
        for t in turni_all:
            # Responsabile
            r_id = t.get('responsabile_id')
            if r_id:
                shift_counts[r_id] = shift_counts.get(r_id, 0) + 1
            
            # Tecnico
            t_id = t.get('tecnico_id')
            if t_id:
                shift_counts[t_id] = shift_counts.get(t_id, 0) + 1
            
            # Volontari List
            for v_id in t.get('volontari_ids', []) or []:
                 shift_counts[v_id] = shift_counts.get(v_id, 0) + 1

        # Apply Filters
        filtered_vol = []
        for v in volontari:
            # Search Text
            full_name = f"{v.get('nome', '')} {v.get('cognome', '')}".lower()
            if filter_text and filter_text.lower() not in full_name:
                continue
            
            # Search Role
            v_roles = v.get('ruoli', [])
            if filter_role:
                # Check if user has AT LEAST one of the selected roles
                if not any(r in v_roles for r in filter_role):
                    continue
            
            filtered_vol.append(v)
            
        # Display List
        st.markdown("---")
        for v in filtered_vol:
            v_id = v['id']
            nome = f"{v.get('nome')} {v.get('cognome')}"
            roles = v.get('ruoli', [])
            
            # Badges
            badges = []
            if "Responsabile" in roles: badges.append("🟠 Resp")
            if "Tecnico" in roles: badges.append("🟢 Tec")
            if "Volontario" in roles: badges.append("🔵 Vol") # Optional, maybe too noisy if everyone is vol
            
            badges_str = " ".join(badges)
            
            count = shift_counts.get(v_id, 0)
            
            with st.container(border=True):
                st.markdown(f"**{nome}**")
                if badges_str:
                    st.caption(badges_str)
                st.write(f"🎯 {count} turni")


    # --- 3. Main Area Logic (Pianificazione) ---
    with col_main:
        st.markdown("### 🗓️ Pianificazione Turni")
        
        # Add Button (Popover)
        with st.popover("➕ Nuovo Turno"):
             st.markdown("#### Crea Nuovo Turno")
             with st.form("form_new_shift"):
                 d_data = st.date_input("Data", value=datetime.now())
                 d_ora = st.time_input("Ora Inizio", value=pd.Timestamp("21:00").time())
                 submitted_shift = st.form_submit_button("Crea Turno")
                 if submitted_shift:
                     if add_turno(d_data, d_ora.strftime("%H:%M")):
                         st.success("Turno creato!")
                         st.rerun()

        st.markdown("---")
        
        # Iterate Shifts
        # Sort logic handles in get_turni (SQL order), but verify
        
        # Helper lists for selects
        # Responsabili: Role 'Responsabile'
        candidate_resp = [v for v in volontari if "Responsabile" in v.get('ruoli', [])]
        # Tecnici: Role 'Tecnico'
        candidate_tec = [v for v in volontari if "Tecnico" in v.get('ruoli', [])]
        # Volontari: All (or filtered?) - Spec says All
        candidate_vol = volontari
        
        # Mappings for UI (ID -> Name)
        # We need options map for Selectbox: {ID: "Name Surname"}
        
        def get_name(v): return f"{v.get('nome')} {v.get('cognome')}"

        map_resp = {v['id']: get_name(v) for v in candidate_resp}
        map_tec = {v['id']: get_name(v) for v in candidate_tec}
        map_vol = {v['id']: get_name(v) for v in candidate_vol} # All
        
        for turno in turni_all:
            t_id = turno['id']
            t_date = pd.to_datetime(turno['data'])
            t_time = turno['ora_inizio'] 
            # Format: Venerdì 06/12/2025 - Ore 21:00
            # Italian locale might not be everywhere, use manual day map or simple formatting
            days_it = ["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"]
            day_name = days_it[t_date.weekday()]
            header_str = f"{day_name} {t_date.strftime('%d/%m/%Y')} - Ore {t_time[:5]}"
            
            # Container
            with st.container(border=True):
                # Using columns for the logic slots
                # Header
                st.markdown(f"##### {header_str}")
                
                c1, c2, c3 = st.columns(3)
                
                # SLOT 1: RESPONSABILE
                curr_resp = turno.get('responsabile_id')
                with c1:
                    sel_resp = st.selectbox(
                        "🟠 Responsabile",
                        options=[None] + list(map_resp.keys()),
                        format_func=lambda x: map_resp[x] if x else "Seleziona...",
                        key=f"resp_{t_id}",
                        index=list(map_resp.keys()).index(curr_resp) + 1 if curr_resp in map_resp else 0
                    )
                    if sel_resp != curr_resp:
                        update_turno_staff(t_id, "responsabile_id", sel_resp)
                        st.rerun()
                
                # SLOT 2: TECNICO
                curr_tec = turno.get('tecnico_id')
                with c2:
                    sel_tec = st.selectbox(
                        "🟢 Tecnico",
                        options=[None] + list(map_tec.keys()),
                        format_func=lambda x: map_tec[x] if x else "Seleziona...",
                        key=f"tec_{t_id}",
                        index=list(map_tec.keys()).index(curr_tec) + 1 if curr_tec in map_tec else 0
                    )
                    if sel_tec != curr_tec:
                        update_turno_staff(t_id, "tecnico_id", sel_tec)
                        st.rerun()

                # SLOT 3: VOLONTARI (Max 3)
                curr_vols = turno.get('volontari_ids') or []
                with c3:
                    sel_vols = st.multiselect(
                        "🔵 Volontari (Max 3)",
                        options=list(map_vol.keys()),
                        default=[v for v in curr_vols if v in map_vol], # safety filter
                        format_func=lambda x: map_vol[x],
                        key=f"vols_{t_id}"
                    )
                    
                    if len(sel_vols) > 3:
                        st.error("Massimo 3 volontari!")
                        # We don't save if valid is broken? Or we save and let user fix?
                        # Spec says "truncate or warning". Error is strictly warning.
                        # Let's enforce truncate if we want to be strict, or just warn.
                        # We will warn here, but if changed, we need check.
                    
                    # Detection of change
                    # Sets are good for comparison independent of order
                    if set(sel_vols) != set(curr_vols) and len(sel_vols) <= 3:
                        update_turno_staff(t_id, "volontari_ids", sel_vols)
                        st.rerun()

                # Visual Feedback
                has_resp = curr_resp is not None
                has_tec = curr_tec is not None
                has_vol = len(curr_vols) > 0
                
                if has_resp and has_tec and has_vol:
                     st.success("✅ Copertura OK")
                else:
                     missing = []
                     if not has_resp: missing.append("Responsabile")
                     if not has_tec: missing.append("Tecnico")
                     if not has_vol: missing.append("Volontari")
                     st.warning(f"⚠️ Manca: {', '.join(missing)}")


# Funzione per la pagina Proiezioni
def render_proiezioni_page():
    st.title("📽️ Proiezioni")

    global supabase
    if not supabase:
        st.error("Client Supabase non inizializzato.")
        return

    # Step A: Fetch All Titles
    try:
        # Fetch data to extract unique titles
        # We need to query the DB. Optimally we would get distinct titles.
        # Since we can't easily do DISTINCT query via this client, we fetch 'Titolo Evento'.
        response = supabase.table(DB_TABLE_NAME).select('"Titolo Evento"').execute()
        
        titles = []
        if response.data:
            # Extract and unique
            _titles = set()
            for row in response.data:
                t = row.get('Titolo Evento')
                if t:
                    _titles.add(t)
            titles = sorted(list(_titles))
            
    except Exception as e:
        st.error(f"Errore nel recupero dei titoli: {e}")
        titles = []

    # Step B: Search Interface (Autocomplete)
    selected_movie = st.selectbox(
        "Cerca Film",
        options=titles,
        index=None,
        placeholder="Inizia a scrivere il nome del film..."
    )

    # Step C: Data Processing (On Selection)
    if selected_movie:
        try:
            # Query specific fields
            response_movie = supabase.table(DB_TABLE_NAME)\
                .select('Data, "Tot. Presenze", Incasso, "Titolo Evento"')\
                .eq('"Titolo Evento"', selected_movie)\
                .execute()
            
            if response_movie.data:
                df = pd.DataFrame(response_movie.data)
                
                # Normalize columns
                # We expect: Data, Tot. Presenze, Incasso
                
                # Handle Presenze
                if 'Tot. Presenze' in df.columns:
                    df['presenze'] = pd.to_numeric(df['Tot. Presenze'], errors='coerce').fillna(0)
                elif 'Presenze' in df.columns:
                     df['presenze'] = pd.to_numeric(df['Presenze'], errors='coerce').fillna(0)
                else:
                    df['presenze'] = 0

                # Handle Incasso
                if 'Incasso' in df.columns:
                    df['incasso'] = pd.to_numeric(df['Incasso'], errors='coerce').fillna(0)
                else:
                     df['incasso'] = 0
                
                # Handle Data
                df['Data'] = pd.to_datetime(df['Data'])
                
                # Aggregates
                unique_dates = sorted(df['Data'].dropna().unique())
                dates_str_list = [pd.to_datetime(d).strftime('%d/%m/%Y') for d in unique_dates]
                dates_str = ", ".join(dates_str_list)
                
                count_events = len(df)
                total_presenze = int(df['presenze'].sum())
                total_incasso = df['incasso'].sum()
                
                # Step D: Visualization (The Card)
                # Using the requested style snippet
                formatted_incasso = f"€ {total_incasso:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

                st.markdown(f"""
                <div style="background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); margin-top: 20px;">
                    <h3 style="color: #333; margin-top: 0;">{selected_movie}</h3>
                    <hr>
                    <p><strong>📅 Date:</strong> {dates_str}</p>
                    <p><strong>📽️ Numero Proiezioni:</strong> {count_events}</p>
                    <p><strong>👥 Totale Presenze:</strong> {total_presenze}</p>
                    <p><strong>💰 Totale Incasso:</strong> {formatted_incasso}</p>
                </div>
                """, unsafe_allow_html=True)
                
            else:
                st.info("Nessun dato trovato per il film selezionato.")
                
        except Exception as e:
            st.error(f"Errore durante il recupero dei dati del film: {e}")


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

    # 2. Sidebar Navigation
    with st.sidebar:
        # Check for logo
        logo_path = "assets/logo.png"
        if os.path.exists(logo_path):
            st.image(logo_path, width=180)
        else:
            st.markdown("### Dashboard")
        
        st.markdown("---")
        
        # Navigation with Emojis
        selected_page = st.radio(
            "Menu",
            options=["📊 Statistiche", "📑 Riepiloghi", "📽️ Proiezioni", "🗓️ Gestione Turni"],
            label_visibility="collapsed"
        )

    # 3. Settings State Management
    if "settings_view" not in st.session_state:
        st.session_state.settings_view = None

    # Logic: If a settings view is active, use it. Otherwise use sidebar selection.
    # We use a 'reset' mechanism: if user clicks sidebar, we clear settings_view.
    
    # Check if sidebar changed (naive approach or just prioritize settings if set)
    # Better approach: Buttons in Popover set 'settings_view'. Sidebar selection clears it? 
    # Let's stick to the user's requested flow:
    
    active_view = selected_page # Default
    
    if st.session_state.settings_view:
        active_view = st.session_state.settings_view

    # 4. Top Bar (Header)
    col_title, col_config = st.columns([6, 1])
    
    with col_title:
        st.markdown(f"## {active_view}")
        
    with col_config:
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

    # 5. Routing (THE FIX: Match Exact Strings)
    if active_view == "📊 Statistiche":
        render_consulta_page()
        
    elif active_view == "📑 Riepiloghi":
        render_riepiloghi_page()
        
    elif active_view == "📽️ Proiezioni":
        render_proiezioni_page()
        
    elif active_view == "🗓️ Gestione Turni":
        render_turni_page()
        
    # Settings Views (Plain text)
    elif active_view == "Configurazione":
        render_config_page()
        
    elif active_view == "Importa Dati":
        render_import_page()
        
    elif active_view == "Utenti":
        render_users_page()
        
    else:
        st.error(f"Pagina non trovata: {active_view}")

if __name__ == "__main__":
    main()
