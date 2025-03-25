import pandas as pd
import os
import logging
from rapidfuzz import fuzz
import json

os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, filename="logs/preprocessing.log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

def update_foreign_keys(duplicates_map: dict, tables_to_update: str):
    """
    Update foreign keys in related tables and remove duplicate lugares
    
    Args:
        duplicates_map: Dictionary mapping duplicate IDs to canonical IDs
        tables_to_update: Path to JSON file containing tables to update
    """
    if not duplicates_map:
        logger.info("No duplicates to update in foreign keys")
        return
        
    with open(tables_to_update, 'r') as f:
        tables_to_update = json.load(f)
    
    sql_updates = []
    for _, table_info in tables_to_update.items():
        table_name = table_info['tablename']
        id_column = table_info['id_column']
        
        for old_id, new_id in duplicates_map.items():
            sql_updates.append(
                f"UPDATE `{table_name}` SET `{id_column}` = {new_id} "
                f"WHERE `{id_column}` = {old_id};"
            )
    
    for old_id in duplicates_map.keys():
        sql_updates.append(f"DELETE FROM `dbgestor_lugar` WHERE `id` = {old_id};")
    
    os.makedirs('data/sql', exist_ok=True)
    mode = 'a' if os.path.exists('data/sql/update_foreign_keys.sql') else 'w'
    
    logger.info(f"Writing {len(sql_updates)} SQL updates (including {len(duplicates_map)} DELETE statements) to data/sql/update_foreign_keys.sql")
    
    with open('data/sql/update_foreign_keys.sql', mode) as f:
        if mode == 'w':
            f.write("START TRANSACTION;\n\n")
            f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
        
        f.write("-- Update foreign key references\n")
        f.write("\n".join(sql_updates[:-len(duplicates_map)]))  
        f.write("\n\n-- Remove duplicate lugares\n")
        f.write("\n".join(sql_updates[-len(duplicates_map):]))  
        f.write("\n")
        
        if mode == 'w':
            f.write("\nSET FOREIGN_KEY_CHECKS=1;\n\n")
            f.write("COMMIT;\n")

def clean_lugares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean lugares table by removing duplicates and generating SQL updates for related tables
    """
    logger.info(f"Initial number of records: {len(df)}")
    
    df['nombre_lugar'] = df['nombre_lugar'].str.strip()
    df['nombre_lugar'] = df['nombre_lugar'].str.lower()
    df['nombre_lugar'] = df['nombre_lugar'].replace(r'\s+', ' ', regex=True)
    
    duplicates = df.groupby(['nombre_lugar', 'tipo']).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1]
    logger.info(f"Found {len(duplicates)} groups of duplicates")
    
    duplicates_map = {}
    for _, row in duplicates.iterrows():
        dupe_rows = df[(df['nombre_lugar'] == row['nombre_lugar']) & (df['tipo'] == row['tipo'])]
        dupe_ids = sorted(dupe_rows['lugar_id'].tolist())
        canonical_id = dupe_ids[0]
        for old_id in dupe_ids[1:]:
            duplicates_map[old_id] = canonical_id
    
    if duplicates_map:
        update_foreign_keys(duplicates_map, 'conf/tables2update.json')
    
    df = df[~df['lugar_id'].isin(duplicates_map.keys())]
    
    return df

def find_fuzzy_matches(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """
    Find fuzzy matches in lugares names and update otros_nombres before removing duplicates
    """
    duplicates_map = {}
    
    for tipo in df['tipo'].unique():
        tipo_df = df[df['tipo'] == tipo]
        nombres = tipo_df['nombre_lugar'].tolist()
        lugar_ids = tipo_df['lugar_id'].tolist()
        
        for i, (nombre1, id1) in enumerate(zip(nombres, lugar_ids)):
            for j, (nombre2, id2) in enumerate(zip(nombres[i+1:], lugar_ids[i+1:]), i+1):
                similarity = fuzz.ratio(nombre1.lower(), nombre2.lower()) / 100
                
                if similarity >= threshold:
                    canonical_id = min(id1, id2)
                    duplicate_id = max(id1, id2)
                    
                    duplicate_name = df.loc[df['lugar_id'] == duplicate_id, 'nombre_lugar'].iloc[0]
                    canonical_otros_nombres = df.loc[df['lugar_id'] == canonical_id, 'otros_nombres'].iloc[0]
                    
                    if pd.isna(canonical_otros_nombres) or canonical_otros_nombres == '':
                        df.loc[df['lugar_id'] == canonical_id, 'otros_nombres'] = duplicate_name
                    else:
                        otros_nombres_list = [name.strip() for name in canonical_otros_nombres.split(';') if name.strip()]
                        if duplicate_name not in otros_nombres_list:
                            otros_nombres_list.append(duplicate_name)
                            df.loc[df['lugar_id'] == canonical_id, 'otros_nombres'] = ';'.join(otros_nombres_list)
                    
                    duplicates_map[duplicate_id] = canonical_id
                    
                    logger.info(f"Fuzzy match found: '{nombre1}' and '{nombre2}' ({similarity:.2f})")
                    logger.info(f"Keeping ID {canonical_id}, removing ID {duplicate_id}")
                    logger.info(f"Updated otros_nombres: {df.loc[df['lugar_id'] == canonical_id, 'otros_nombres'].iloc[0]}\n")
    
    if duplicates_map:
        update_foreign_keys(duplicates_map, 'conf/tables2update.json')
    
    df = df[~df['lugar_id'].isin(duplicates_map.keys())]
    
    os.makedirs('data/interim', exist_ok=True)
    df.to_csv('data/interim/lugares_processed.csv', index=False)
    
    logger.info(f"\nRemoved {len(duplicates_map)} fuzzy duplicates")
    logger.info(f"Updated DataFrame saved to data/interim/lugares_processed.csv")
    
    return df

def main():
    
    if os.path.exists("data/sql/update_foreign_keys.sql"):
        os.remove("data/sql/update_foreign_keys.sql")
    
    try:
        df = pd.read_csv("data/interim/lugares.csv")
        df['otros_nombres'] = df['otros_nombres'].astype(str)
        df['otros_nombres'] = df['otros_nombres'].replace('nan', '')
        logger.info(f"Lugares before cleaning: {df.shape}")
        
        df = clean_lugares(df)
        df = find_fuzzy_matches(df, threshold=0.9)
        
        logger.info(f"Lugares after cleaning: {df.shape}")
        
    except Exception as e:
        logger.error(f"Error processing lugares: {str(e)}")
        raise
    
    logger.info(f"Lugares after cleaning: {df.shape}")

if __name__ == "__main__":
    main()