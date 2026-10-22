import pandas as pd
import geopandas as gpd
from pymongo import MongoClient, GEOSPHERE
import sys
import os
import time
import warnings
import gc
import numpy as np

# Αγνοούμε warnings για καθαρό output
warnings.filterwarnings("ignore")

# --- 1. MAPPING (Τύποι Πλοίων) ---
SHIP_TYPE_MAPPING = {
    30: "Fishing", 31: "Towing", 32: "Towing", 33: "Dredging", 34: "Diving",
    35: "Military", 36: "Sailing", 37: "Pleasure Craft", 38: "Reserved", 39: "Reserved",
    40: "High Speed Craft", 41: "High Speed Craft", 42: "High Speed Craft", 
    43: "High Speed Craft", 44: "High Speed Craft", 45: "High Speed Craft", 
    46: "High Speed Craft", 47: "High Speed Craft", 48: "High Speed Craft", 49: "High Speed Craft",
    50: "Pilot Vessel", 51: "Search and Rescue", 52: "Tug", 53: "Port Tender",
    54: "Anti-pollution", 55: "Law Enforcement", 56: "Local Vessel", 57: "Local Vessel",
    58: "Medical Transport", 59: "Non-combatant",
    60: "Passenger", 61: "Passenger", 62: "Passenger", 63: "Passenger", 
    64: "Passenger", 65: "Passenger", 66: "Passenger", 67: "Passenger", 
    68: "Passenger", 69: "Passenger",
    70: "Cargo", 71: "Cargo", 72: "Cargo", 73: "Cargo", 74: "Cargo", 
    75: "Cargo", 76: "Cargo", 77: "Cargo", 78: "Cargo", 79: "Cargo",
    80: "Tanker", 81: "Tanker", 82: "Tanker", 83: "Tanker", 84: "Tanker", 
    85: "Tanker", 86: "Tanker", 87: "Tanker", 88: "Tanker", 89: "Tanker",
    90: "Other", 91: "Other", 92: "Other", 93: "Other", 94: "Other", 
    95: "Other", 96: "Other", 97: "Other", 98: "Other", 99: "Other"
}

def get_ship_type_name(type_code):
    try:
        if pd.isna(type_code): return "Unknown"
        return SHIP_TYPE_MAPPING.get(int(float(type_code)), "Unknown")
    except: return "Unknown"

# --- 2. WEATHER LOADING LOGIC ---
MONTH_MAP = { 1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun', 7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec' }
weather_gdf_cache = {}

def get_weather_gdf_for_month(year, month_num):
    """Φορτώνει το Shapefile του μήνα και το κρατάει στη μνήμη (Cache)"""
    month_str = MONTH_MAP.get(month_num)
    if not month_str: return None
    key = f"{year}_{month_str}"
    
    if key in weather_gdf_cache: return weather_gdf_cache[key]
    
    filename = f"noaa_weather_{month_str}{year}_v2.shp"
    file_path = os.path.join('noaa_weather', str(year), month_str, filename)
    
    if os.path.exists(file_path):
        try:
            print(f"   [INFO] Φόρτωση αρχείου καιρού: {filename} ...")
            gdf = gpd.read_file(file_path)
            
            if gdf.crs is None: gdf.set_crs(epsg=4326, inplace=True)
            else: gdf = gdf.to_crs(epsg=4326)
            
            _ = gdf.sindex 
            
            possible_time_cols = ['timestamp', 'datetime', 'DATE', 'TIME', 'dt']
            time_col = next((x for x in possible_time_cols if x in gdf.columns), None)
            
            if time_col:
                gdf[time_col] = pd.to_datetime(gdf[time_col])
                gdf = gdf.rename(columns={time_col: 'weather_ts'})
            
            weather_gdf_cache[key] = gdf
            return gdf
        except Exception as e:
            print(f"   [ERROR] Σφάλμα φόρτωσης καιρού: {e}")
            return None
    else: 
        return None

# --- 3. SETUP MONGODB ---
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27018/')
print("--> Αναμονή 5 δευτερολέπτων για εκκίνηση MongoDB...")
time.sleep(5)

try:
    client = MongoClient(mongo_uri)
    db = client['MaritimeDB_Advanced_Final']
    collection = db['trajectories_temporal']
    client.admin.command('ping')
    print("--> Σύνδεση με MongoDB Επιτυχής!")
except Exception as e:
    print(f"ΣΦΑΛΜΑ ΣΥΝΔΕΣΗΣ: {e}")
    sys.exit(1)

# Static Data
static_map = {}
if os.path.exists('ais_static/unipi_ais_static.csv'):
    try:
        df_s = pd.read_csv('ais_static/unipi_ais_static.csv', header=None, names=['id','co','type'])
        for _, r in df_s.iterrows():
            static_map[str(r['id']).strip()] = { "country": str(r['co']), "type": get_ship_type_name(r['type']) }
    except: pass

if not os.path.exists('ais_data.csv'):
    print("Missing ais_data.csv")
    sys.exit(1)

# --- 4. CHUNK PROCESSING (SPATIO-TEMPORAL MODE) ---
CHUNK_SIZE = 10000 
print(f"--> Έναρξη Advanced Processing (Chunk Size: {CHUNK_SIZE})...")

chunk_iterator = pd.read_csv('ais_data.csv', chunksize=CHUNK_SIZE)
total_processed_chunks = 0
total_rows_saved = 0

for chunk_df in chunk_iterator:
    chunk_df['timestamp'] = pd.to_datetime(chunk_df['t'], unit='ms')
    chunk_df = chunk_df[chunk_df['timestamp'].dt.month.isin([6, 7, 8, 9])]
    
    if chunk_df.empty:
        del chunk_df
        continue

    # --- ADVANCED WEATHER LOGIC START ---
    min_ts = chunk_df['timestamp'].min()
    max_ts = chunk_df['timestamp'].max()
    weather_gdf = get_weather_gdf_for_month(min_ts.year, min_ts.month)
    
    source_df = chunk_df
    weather_found = False
    
    if weather_gdf is not None and 'weather_ts' in weather_gdf.columns:
        try:
            # A. Smart Filter (+/- 3 ώρες)
            buffer_start = min_ts - pd.Timedelta(hours=3)
            buffer_end = max_ts + pd.Timedelta(hours=3)
            
            weather_subset = weather_gdf[
                (weather_gdf['weather_ts'] >= buffer_start) & 
                (weather_gdf['weather_ts'] <= buffer_end)
            ].copy()
            
            if not weather_subset.empty:
                # B. Spatial Join
                gdf_points = gpd.GeoDataFrame(
                    chunk_df, 
                    geometry=gpd.points_from_xy(chunk_df.lon, chunk_df.lat),
                    crs="EPSG:4326"
                )
                gdf_points['original_index'] = gdf_points.index

                joined = gpd.sjoin_nearest(gdf_points, weather_subset, how="left", max_distance=0.15, distance_col="dist")
                
                # C. Temporal Check
                if 'weather_ts' in joined.columns:
                    joined['time_diff'] = (joined['timestamp'] - joined['weather_ts']).abs()
                    joined = joined.sort_values(by=['original_index', 'time_diff', 'dist'])
                    joined = joined.drop_duplicates(subset=['original_index'], keep='first')
                
                # --- ΔΙΟΡΘΩΣΗ: Rename SUFFIXES ---
                # Επαναφέρουμε τα ονόματα αν έχουν αλλάξει λόγω του join
                rename_dict = {}
                if 'lon' not in joined.columns and 'lon_left' in joined.columns:
                    rename_dict['lon_left'] = 'lon'
                if 'lat' not in joined.columns and 'lat_left' in joined.columns:
                    rename_dict['lat_left'] = 'lat'
                if 'speed' not in joined.columns and 'speed_left' in joined.columns:
                    rename_dict['speed_left'] = 'speed'
                if 'course' not in joined.columns and 'course_left' in joined.columns:
                    rename_dict['course_left'] = 'course'
                if 'heading' not in joined.columns and 'heading_left' in joined.columns:
                    rename_dict['heading_left'] = 'heading'
                
                if rename_dict:
                    joined = joined.rename(columns=rename_dict)
                # -----------------------------------

                cols_to_keep = list(chunk_df.columns) # Λίστα για ασφάλεια
                
                # Προσθέτουμε τις στήλες καιρού που θέλουμε
                weather_cols = ['TMP', 'WSPD', 'GUST', 'RH', 'PRMSL', 'weather_ts', 'dist', 'time_diff']
                for wc in weather_cols:
                    if wc in joined.columns:
                        cols_to_keep.append(wc)
                
                # Επιλέγουμε μόνο τις στήλες που όντως υπάρχουν
                existing_cols = [c for c in cols_to_keep if c in joined.columns]
                source_df = joined[existing_cols].copy()
                
                weather_found = True
                del gdf_points, joined, weather_subset
                
        except Exception as e:
            print(f"Warning in Spatio-Temporal Join: {e}")
            source_df = chunk_df
    
    # --- ADVANCED WEATHER LOGIC END ---

    source_df['bucket'] = source_df['timestamp'].dt.floor('6h')
    
    try:
        grouped = source_df.groupby(['vessel_id', 'bucket'])
    except KeyError: 
        del chunk_df, source_df
        continue

    mongo_docs = []
    
    for (v_id, bucket_ts), group in grouped:
        try: group = group.sort_values('timestamp')
        except: pass
        
        points = []
        for idx, row in group.iterrows():
            try:
                ts = row.get('timestamp')
                lon = row.get('lon')
                lat = row.get('lat')
                if pd.isna(ts) or pd.isna(lon) or pd.isna(lat): continue
                
                p = {
                    "ts": ts,
                    "location": { "type": "Point", "coordinates": [float(lon), float(lat)] },
                    "speed": float(row.get('speed', 0)) if pd.notna(row.get('speed', 0)) else 0.0,
                    "heading": float(row.get('heading', 0)) if pd.notna(row.get('heading', 0)) else 0.0,
                    "course": float(row.get('course', 0)) if pd.notna(row.get('course', 0)) else 0.0
                }
                
                if weather_found:
                    weather_data = {}
                    if pd.notna(row.get('TMP')):
                        try: weather_data['temp_c'] = round(float(row['TMP']) - 273.15, 1)
                        except: pass
                    if pd.notna(row.get('WSPD')): weather_data['wind_speed'] = round(float(row['WSPD']), 1)
                    if pd.notna(row.get('GUST')): weather_data['wind_gust'] = round(float(row['GUST']), 1)
                    if pd.notna(row.get('RH')): weather_data['humidity'] = float(row['RH'])
                    if pd.notna(row.get('PRMSL')): weather_data['pressure'] = float(row['PRMSL'])
                    
                    if pd.notna(row.get('time_diff')):
                         minutes_diff = row['time_diff'].total_seconds() / 60
                         weather_data['time_offset_min'] = round(minutes_diff, 1)

                    if weather_data: p["weather"] = weather_data
                
                points.append(p)
            except: continue
        
        if not points: continue
        
        st_dat = static_map.get(str(v_id), {"country": "Unknown", "type": "Unknown"})
        doc = {
            "ship_id": str(v_id),
            "static_data": { "country": st_dat['country'], "vessel_type": st_dat['type'] },
            "bucket_period": { "start": group['timestamp'].min(), "end": group['timestamp'].max() },
            "points": points,
            "point_count": len(points)
        }
        mongo_docs.append(doc)
    
    if mongo_docs:
        collection.insert_many(mongo_docs)
        total_rows_saved += len(mongo_docs)
    
    total_processed_chunks += 1
    print(f"--> Chunk #{total_processed_chunks} ΟΚ (Total saved: {total_rows_saved})")
    
    del chunk_df, source_df, mongo_docs, grouped
    gc.collect()

print("--> Δημιουργία Indexes...")
collection.create_index([("points.location", GEOSPHERE)])
collection.create_index([("ship_id", "hashed")])
collection.create_index([("bucket_period.start", 1)])
collection.create_index([("static_data.vessel_type", 1)])
print("--> ΤΕΛΟΣ ΕΡΓΑΣΙΑΣ (TEMPORAL MODE)!")