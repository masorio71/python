import pandas as pd
import numpy as np

def test_logic():
    print("--- Starting Verification ---")
    
    # Mock Data
    data = {
        'Data': ['2023-01-01', '2023-06-01', '2023-08-15', '2023-09-15', '2023-09-16', '2023-12-31'],
        'Titolo Evento': ['Event A', 'Event B', 'TITOLO DI PROVA SIAE', 'Event C', 'Event D', 'TITOLO DI PROVA SIAE'],
        'Incasso': [100, 200, 0, 300, 400, 0]
    }
    df = pd.DataFrame(data)
    event_name_col = 'Titolo Evento'
    
    print(f"Initial Rows: {len(df)}")
    
    # 1. Row Exclusion Logic
    if event_name_col in df.columns:
        initial_count = len(df)
        df = df[df[event_name_col] != "TITOLO DI PROVA SIAE"]
        filtered_count = len(df)
        print(f"Filtered Rows: {filtered_count}")
        print(f"Excluded: {initial_count - filtered_count}")
        
    # Verify exclusion
    assert "TITOLO DI PROVA SIAE" not in df['Titolo Evento'].values
    assert len(df) == 4
    print("✅ Row Exclusion Logic Passed")

    # 2. Rassegna Logic
    def get_rassegna(row):
        try:
            dt = pd.to_datetime(row['Data'])
            md = (dt.month, dt.day)
            if (6, 1) <= md <= (9, 15):
                return "CASTELLO"
            else:
                return "STANDARD"
        except:
            return "STANDARD"

    df['RASSEGNA'] = df.apply(get_rassegna, axis=1)
    
    # Verify Rassegna
    # 2023-01-01 -> STANDARD
    # 2023-06-01 -> CASTELLO
    # 2023-09-15 -> CASTELLO
    # 2023-09-16 -> STANDARD
    
    expected_rassegna = ['STANDARD', 'CASTELLO', 'CASTELLO', 'STANDARD']
    actual_rassegna = df['RASSEGNA'].tolist()
    
    print(f"Expected Rassegna: {expected_rassegna}")
    print(f"Actual Rassegna:   {actual_rassegna}")
    
    assert actual_rassegna == expected_rassegna
    print("✅ Rassegna Logic Passed")
    
    # 3. Anti-Duplication Logic Simulation
    existing_db = {
        ('2023-01-01', 'Event A'),
        ('2023-06-01', 'Event B')
    }
    
    new_records = []
    skipped_count = 0
    
    for index, row in df.iterrows():
        row_date = row['Data']
        row_title = row[event_name_col]
        
        if (row_date, row_title) in existing_db:
            skipped_count += 1
        else:
            new_records.append(row.to_dict())
            
    print(f"Skipped (Duplicates): {skipped_count}")
    print(f"New Records to Insert: {len(new_records)}")
    
    # Expect 2 skipped (Event A, Event B) and 2 new (Event C, Event D)
    assert skipped_count == 2
    assert len(new_records) == 2
    assert new_records[0]['Titolo Evento'] == 'Event C'
    assert new_records[1]['Titolo Evento'] == 'Event D'
    print("✅ Anti-Duplication Logic Passed")
    
    print("--- All Tests Passed ---")

def test_fiscali_logic():
    print("\n--- Starting Fiscali Verification ---")
    from datetime import datetime
    
    # Mock Data imitating ExportTitoliFiscali structure
    # Column indices simulation: 2=Date, 7=Title, 21=TicketType
    # We will just use a list of dicts or objects to simulate row access if using iterrows, 
    # but since the app uses iloc on a dataframe, we should build a dataframe.
    
    data = []
    # Row 1: 15:00
    row1 = [None] * 25
    row1[2] = "01/01/2023 15.00.00"
    row1[7] = "Movie A"
    row1[21] = "I1"
    data.append(row1)
    
    # Row 2: 15:00 (Same time, same movie -> should aggregate with Row 1)
    row2 = [None] * 25
    row2[2] = "01/01/2023 15.00.00"
    row2[7] = "Movie A"
    row2[21] = "R7"
    data.append(row2)
    
    # Row 3: 17:00 (Different time, same movie -> New Group)
    row3 = [None] * 25
    row3[2] = "01/01/2023 17.00.00"
    row3[7] = "Movie A"
    row3[21] = "I1"
    data.append(row3)
    
    # Row 4: Different Movie
    row4 = [None] * 25
    row4[2] = "01/01/2023 21.00.00"
    row4[7] = "Movie B"
    row4[21] = "I1"
    data.append(row4)

    df_fiscali = pd.DataFrame(data)
    
    agg_map = {}
    
    for idx, row in df_fiscali.iterrows():
        raw_val = row.iloc[2]
        
        # LOGIC FROM APP.PY (Simulation)
        dt_obj = datetime.strptime(raw_val, "%d/%m/%Y %H.%M.%S")
        title = row.iloc[7]
        ticket_val = str(row.iloc[21])
        
        # NEW KEY LOGIC
        time_str = dt_obj.strftime('%H:%M')
        key = (title, dt_obj.date(), time_str)
        
        if key not in agg_map:
            agg_map[key] = {
                "titolo_evento": title,
                "data": dt_obj.date(),
                "orario": time_str, # NEW FIELD
                "interi": 0, "ridotti": 0
            }
        
        stats = agg_map[key]
        if ticket_val == 'I1':
            stats['interi'] += 1
        elif ticket_val == 'R7':
            stats['ridotti'] += 1
            
    print(f"Aggregated Groups: {len(agg_map)}")
    # We expect 3 groups: 
    # 1. Movie A at 15:00 (2 tickets: 1 interi, 1 ridotti)
    # 2. Movie A at 17:00 (1 ticket)
    # 3. Movie B at 21:00 (1 ticket)
    
    assert len(agg_map) == 3
    
    # Check Group 1
    key1 = ("Movie A", datetime(2023, 1, 1).date(), "15:00")
    assert key1 in agg_map
    assert agg_map[key1]['interi'] == 1
    assert agg_map[key1]['ridotti'] == 1
    assert agg_map[key1]['orario'] == "15:00"
    
    # Check Group 2
    key2 = ("Movie A", datetime(2023, 1, 1).date(), "17:00")
    assert key2 in agg_map
    assert agg_map[key2]['orario'] == "17:00"
    
    print("✅ Fiscali Grouping by Time Passed")


if __name__ == "__main__":
    test_logic()
    test_fiscali_logic()
