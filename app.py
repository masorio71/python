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
    initial_sidebar_state="collapsed"
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

    # Upload File Excel
    st.subheader("Carica Dati Excel")
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
            
            def get_evento(row):
                dt = row['temp_datetime']
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

            selected_columns['Evento'] = selected_columns.apply(get_evento, axis=1)

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
            # Filter out "TITOLO DI PROVA SIAE" and "Verifica Conformità"
            if event_name_col in df_uploaded.columns:
                initial_count = len(df_uploaded)
                banned_titles = ["TITOLO DI PROVA SIAE", "Verifica Conformità"]
                # Filter rows where title is NOT in banned_titles
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
            
            if st.button("Carica Dati su Supabase"):
                if not supabase:
                    st.error("Client Supabase non inizializzato. Controlla le credenziali.")
                else:
                    try:
                        # --- 3. Anti-Duplication Logic ---
                        # Fetch existing records (Data, Titolo Evento)
                        existing_data_response = supabase.table(DB_TABLE_NAME).select("Data", f'"{event_name_col}"').execute()
                        existing_records = existing_data_response.data if existing_data_response.data else []
                        
                        # Create a set of tuples (date_string, title_string) for O(1) lookup
                        # Ensure date format matches. Supabase returns YYYY-MM-DD. df_uploaded['Data'] is YYYY-MM-DD.
                        existing_set = set()
                        for record in existing_records:
                            r_date = record.get('Data')
                            r_title = record.get(event_name_col)
                            if r_date and r_title:
                                existing_set.add((r_date, r_title))
                        
                        # Filter out duplicates
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
                            # Insert ONLY new records
                            response = supabase.table(DB_TABLE_NAME).insert(new_records).execute()
                            st.success(f"Caricamento completato! Inserite {len(new_records)} nuove righe.")
                            if skipped_count > 0:
                                st.warning(f"Saltate {skipped_count} righe perché già presenti nel database (duplicati Data + Titolo).")
                            # st.json(response.data[0] if response.data else {})
                        else:
                            st.warning(f"Nessuna nuova riga da inserire. Tutte le {skipped_count} righe sono già presenti nel database.")

                    except Exception as e:
                        error_msg = str(e)
                        if "PGRST205" in error_msg or "Could not find the table" in error_msg:
                            st.error(f"Errore: La tabella '{DB_TABLE_NAME}' non esiste in Supabase.")
                            st.warning("Supabase non crea automaticamente le tabelle tramite API. Esegui questo SQL nell'editor SQL di Supabase per creare la tabella:")
                            
                            # Generate SQL CREATE TABLE statement
                            sql_columns = []
                            for col_name, dtype in df_uploaded.dtypes.items():
                                # Map pandas/numpy types to PostgreSQL types
                                if col_name == "Data":
                                    pg_type = "DATE"
                                elif pd.api.types.is_integer_dtype(dtype):
                                    pg_type = "BIGINT"
                                elif pd.api.types.is_float_dtype(dtype):
                                    pg_type = "NUMERIC"
                                elif pd.api.types.is_bool_dtype(dtype):
                                    pg_type = "BOOLEAN"
                                elif pd.api.types.is_datetime64_any_dtype(dtype):
                                    pg_type = "TIMESTAMP"
                                else:
                                    pg_type = "TEXT"
                                
                                # Sanitize column name (simple version)
                                safe_col_name = f'"{col_name}"'
                                sql_columns.append(f"    {safe_col_name} {pg_type}")
                            
                            create_table_sql = f"CREATE TABLE public.{DB_TABLE_NAME} (\n"
                            create_table_sql += "    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n"
                            create_table_sql += ",\n".join(sql_columns)
                            create_table_sql += "\n);"
                            
                            st.code(create_table_sql, language="sql")
                            
                        else:
                            st.error(f"Errore durante il caricamento: {e}")
                            st.info("Nota: Assicurati che la tabella esista in Supabase e che le colonne corrispondano.")
                        
        except Exception as e:
            st.error(f"Errore nella lettura del file: {e}")
    else:
        st.info("Carica un file Excel per iniziare.")

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
def render_kpi_card(title, value, comparison_html, details, border_color="gray"):
    st.markdown(f"""
<div style="background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 5px solid {border_color};">
    <div style="font-size: 12px; font-weight: bold; color: #888; text-transform: uppercase; letter-spacing: 1px;">
        {title}
    </div>
    <div style="font-size: 36px; font-weight: bold; color: #333; margin: 10px 0;">
        {value}
    </div>
    <div style="font-size: 14px; margin-bottom: 8px;">
        {comparison_html}
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

    # Inizializza session state
    if 'consulta_data' not in st.session_state:
        st.session_state.consulta_data = None
    if 'consulta_data_p2' not in st.session_state:
        st.session_state.consulta_data_p2 = None
    if 'active_filter' not in st.session_state:
        st.session_state.active_filter = 'TOTALE'

    if 'is_comparison' not in st.session_state:
        st.session_state.is_comparison = False

    # Sidebar settings
    st.sidebar.subheader("Impostazioni")
    # Removed table name input

    # Logica Elaborazione
    if elabora_btn:
        st.session_state.is_comparison = comparison_mode
        try:
            # Fetch Period 1 (Real Data)
            response = supabase.table(DB_TABLE_NAME) \
                .select("*") \
                .gte("Data", start_date.strftime('%Y-%m-%d')) \
                .lte("Data", end_date.strftime('%Y-%m-%d')) \
                .execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                if 'Incasso' in df.columns:
                    df['Incasso'] = pd.to_numeric(df['Incasso'], errors='coerce').fillna(0)
                st.session_state.consulta_data = df
            else:
                st.warning("Nessun dato trovato nel Periodo 1.")
                st.session_state.consulta_data = pd.DataFrame()

            # Fetch/Generate Period 2 (Mock Data) if comparison is ON
            if comparison_mode:
                # Generate Mock Data
                df_p2 = generate_mock_data(start_date_p2, end_date_p2)
                st.session_state.consulta_data_p2 = df_p2
            else:
                st.session_state.consulta_data_p2 = None
                
            st.session_state.active_filter = 'TOTALE'
            
        except Exception as e:
            st.error(f"Errore durante il recupero dei dati: {e}")
            st.session_state.consulta_data = pd.DataFrame()
            st.session_state.consulta_data_p2 = None

    # --- KPI Section ---
    if st.session_state.consulta_data is not None and not st.session_state.consulta_data.empty:
        df = st.session_state.consulta_data
        metrics_p1 = calculate_metrics(df)
        
        metrics_p2 = None
        if st.session_state.is_comparison and st.session_state.consulta_data_p2 is not None:
             metrics_p2 = calculate_metrics(st.session_state.consulta_data_p2)

        # Render KPI Cards
        st.markdown("###") # Spacer
        cols = st.columns(4)
        
        order = ['TOTALE', 'ADULTI', 'VENERDÌ', 'BAMBINI']
        
        for i, key in enumerate(order):
            m1 = metrics_p1[key]
            
            comparison_html = ""
            if metrics_p2:
                m2 = metrics_p2[key]
                val1 = m1['incasso']
                val2 = m2['incasso']
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
                
                comparison_html = f'<span style="color: {color}; font-weight: bold;">{arrow} {delta_percent:+.1f}%</span> vs Periodo 2'
            else:
                comparison_html = "&nbsp;"

            presenze_formatted = f"{int(m1['presenze']):,}".replace(",", ".")
            avg_ticket_formatted = format_euro_precise(m1['avg_ticket'])
            details = f"<strong>{presenze_formatted}</strong> Presenze<br>Biglietto medio: <strong>{avg_ticket_formatted}</strong> • {m1['count']} Eventi"

            with cols[i]:
                render_kpi_card(
                    title=key,
                    value=format_euro(m1['incasso']),
                    comparison_html=comparison_html,
                    details=details,
                    border_color=m1['color']
                )

        # --- Filter Selection & Data Table (Only if NOT comparison) ---
        if not st.session_state.is_comparison:
            st.markdown("###")
            st.markdown("### Dettaglio Analitico")
            
            # Filtro visivo
            # Layout a due colonne: Filtri a sinistra, Metrica a destra
            col_filter, col_metric = st.columns([3, 1])
            
            with col_filter:
                st.radio(
                    "Filtra Tabella:",
                    options=order,
                    horizontal=True,
                    label_visibility="collapsed",
                    key="active_filter"
                )

            # Calcolo Metrica Dinamica "Media Ingressi"
            # Logica duplicata ma necessaria per reattività immediata post-click
            kpi_configs_metric = { 
                'TOTALE': {'filter': None},
                'ADULTI': {'filter': 'adu'},
                'VENERDÌ': {'filter': 'ven'},
                'BAMBINI': {'filter': 'bam'}
            }
            
            current_config_metric = kpi_configs_metric[st.session_state.active_filter]
            if current_config_metric['filter']:
                subset_metric = df[df['Evento'] == current_config_metric['filter']]
            else:
                subset_metric = df
            
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
            
            # Filtra DF
            kpi_configs = { 
                'TOTALE': {'filter': None},
                'ADULTI': {'filter': 'adu'},
                'VENERDÌ': {'filter': 'ven'},
                'BAMBINI': {'filter': 'bam'}
            }
            
            current_config = kpi_configs[st.session_state.active_filter]
            if current_config['filter']:
                display_df = df[df['Evento'] == current_config['filter']].copy()
            else:
                display_df = df.copy()

            # Filtro VOS opzionale
            if 'VOS' in display_df.columns and display_df['VOS'].any():
                if st.checkbox("Mostra solo Versione Originale (VOS)", value=False):
                    display_df = display_df[display_df['VOS'] == True]

            # Pulizia colonne per display
            cols_to_drop = ['id', 'Evento', 'VOS', 'temp_datetime']
            display_df = display_df.drop(columns=[c for c in cols_to_drop if c in display_df.columns])

            # Formattazione Colonne
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

            # --- PIE CHART SECTION ---
            if 'Nazionalità' in display_df.columns and 'Incasso' in display_df.columns and not display_df.empty:
                st.markdown("### Ripartizione Incassi per Nazionalità")
                
                # 1. Prepare Data
                # Ensure Incasso is numeric
                pie_df = display_df.copy()
                pie_df['Incasso'] = pd.to_numeric(pie_df['Incasso'], errors='coerce').fillna(0)
                
                # Group initially
                pie_grouped = pie_df.groupby('Nazionalità', as_index=False)['Incasso'].sum()
                total_incasso = pie_grouped['Incasso'].sum()
                
                # 2. Apply "Altri" logic (< 5%)
                if total_incasso > 0:
                    pie_grouped['Nazionalità'] = pie_grouped.apply(
                        lambda x: x['Nazionalità'] if (x['Incasso'] / total_incasso) >= 0.05 else 'Altri', 
                        axis=1
                    )
                    # Re-group to merge 'Altri'
                    pie_final = pie_grouped.groupby('Nazionalità', as_index=False)['Incasso'].sum()
                    
                    # Sort by Incasso descending
                    pie_final = pie_final.sort_values(by='Incasso', ascending=False)
                    
                    # Limit to top 10 (safety)
                    pie_final = pie_final.head(10)
                    
                    # 3. Visualization (Plotly)
                    fig = px.pie(
                        pie_final, 
                        values='Incasso', 
                        names='Nazionalità',
                        title='Ripartizione Incassi per Nazionalità'
                    )
                    fig.update_traces(textinfo='label+percent')
                    
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("La tabella dettagliata è nascosta in modalità confronto.")

    elif st.session_state.consulta_data is not None and st.session_state.consulta_data.empty:
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
def main():
    # CSS Injection per stile globale, top bar e nascondere sidebar
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap');
        
        /* Background App */
        .stApp {
            background-color: #F0F2F6;
            font-family: 'Roboto', sans-serif;
        }
        
        /* Nascondi Sidebar */
        [data-testid="stSidebar"] {
            display: none;
        }
        
        /* Top Bar Container */
        .top-bar {
            background-color: white;
            padding: 10px 20px;
            border-bottom: 1px solid #ddd;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        /* Header H3 pulito */
        h3 {
            color: #202124;
            font-weight: 500;
            padding-top: 20px;
        }
        
        /* Rimuovi padding eccessivo standard di Streamlit */
        .block-container {
            padding-top: 1rem; /* Ridotto per la top bar */
            padding-bottom: 2rem;
        }
        
        /* Nascondi elementi standard */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* --- GOOGLE STYLE TABS (Radio Button Hack) --- */
        
        /* 1. Hide the Radio Circles (Pallini) CRITICAL */
        div[role="radiogroup"] > label > div:first-child {
            display: none !important;
        }
        
        /* 2. Style the Labels (Buttons) */
        div[role="radiogroup"] {
            display: flex;
            flex-direction: row;
            gap: 20px; /* Spazio tra i tab */
            justify-content: flex-end; /* Allinea a destra */
        }
        
        div[role="radiogroup"] > label {
            background-color: transparent !important;
            border: none !important;
            color: #5f6368 !important; /* Google Gray */
            font-weight: 500 !important;
            font-size: 14px !important;
            padding: 10px 16px !important;
            cursor: pointer !important;
            transition: all 0.2s ease-in-out;
            margin-right: 0px !important;
            border-bottom: 3px solid transparent !important; /* Placeholder per bordo */
        }
        
        div[role="radiogroup"] > label:hover {
            color: #1a73e8 !important; /* Google Blue */
            background-color: rgba(26, 115, 232, 0.04) !important;
            border-radius: 4px 4px 0 0;
        }
        
        /* 3. Selected State */
        div[role="radiogroup"] > label[data-checked="true"] {
            color: #1a73e8 !important; /* Google Blue */
            border-bottom: 3px solid #1a73e8 !important; /* Blue Underline */
            border-radius: 4px 4px 0 0 !important;
        }
        
        </style>
    """, unsafe_allow_html=True)

    # --- Top Header Layout ---
    with st.container():
        # Layout: Logo (1) | Spacer (4) | Nav (2)
        col_logo, col_spacer, col_nav = st.columns([1, 4, 2])
        
        with col_logo:
            # Check for logo file in assets
            logo_path = "assets/logo.png"
            
            # Fallback logic if user hasn't created folder yet (or old path)
            if not os.path.exists(logo_path):
                 if os.path.exists("Metropol marchio TRACC.ai.png"):
                     logo_path = "Metropol marchio TRACC.ai.png"
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=150)
            else:
                st.markdown("### Metropol TRACC.ai")

        with col_nav:
            # Navigation Menu (Radio Buttons styled as Tabs)
            selected_page = st.radio(
                "Navigazione",
                options=["Consulta Dati", "⚙️"],
                horizontal=True,
                label_visibility="collapsed",
                key="main_nav_selection"
            )

    st.markdown("---") # Separator

    # --- Page Routing ---
    if selected_page == "Consulta Dati":
        render_consulta_page()
    
    elif selected_page == "⚙️":
        st.title("⚙️ Impostazioni")
        
        tab1, tab2, tab3 = st.tabs(["Configurazione", "Importa Dati", "Utenti"])
        
        with tab1:
            render_config_page()
        
        with tab2:
            render_import_page()
        
        with tab3:
            render_users_page()

if __name__ == "__main__":
    main()
