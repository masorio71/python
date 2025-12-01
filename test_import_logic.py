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

if __name__ == "__main__":
    test_logic()
