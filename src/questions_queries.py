from pymongo import MongoClient
import datetime
import time
from math import radians, cos, sin, asin, sqrt

# --- ΡΥΘΜΙΣΕΙΣ ΣΥΝΔΕΣΗΣ (V2 PROJECT) ---
# Συνδεόμαστε στη θύρα 27018 όπου τρέχει η νέα βάση με τον χρόνο
client = MongoClient('mongodb://localhost:27018/')
db = client['MaritimeDB_Advanced_Final']
collection = db['trajectories_temporal']

print("---  ΕΝΑΡΞΗ ANALYTICS & BENCHMARKING (V2 - SPATIOTEMPORAL) ---\n")

# ======================================================
# 1. ΚΑΤΑΝΟΜΗ ΔΕΔΟΜΕΝΩΝ
# ======================================================
print("1. [Q1] Συνολικά Στίγματα ανά Τύπο Πλοίου")
start_time = time.time()

pipeline_q1 = [
    {
        "$group": {
            "_id": "$static_data.vessel_type",
            "total_positions": { "$sum": "$point_count" } 
        }
    },
    { "$sort": { "total_positions": -1 } }
]

results_q1 = list(collection.aggregate(pipeline_q1))

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
for r in results_q1:
    print(f"   - {r['_id']}: {r['total_positions']:,}")


# ======================================================
# 2. ΑΝΑΛΥΣΗ ΤΑΧΥΤΗΤΑΣ
# ======================================================
print("\n2. [Q2] Μέγιστη Ταχύτητα ανά Τύπο (Top 5)")
start_time = time.time()

pipeline_q2 = [
    { "$unwind": "$points" }, 
    # Φίλτρο: Αγνοούμε ταχύτητες > 55 κόμβων (θεωρούνται θόρυβος GPS)
    { "$match": { "points.speed": { "$lt": 55 } } }, 
    {
        "$group": {
            "_id": "$static_data.vessel_type",
            "max_speed": { "$max": "$points.speed" }
        }
    },
    { "$sort": { "max_speed": -1 } }
]

results_q2 = list(collection.aggregate(pipeline_q2))

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
for r in results_q2[:5]:
    print(f"   - {r['_id']}: {r['max_speed']} knots")


# ======================================================
# 3. ΥΠΟΛΟΓΙΣΤΙΚΟ ΕΡΩΤΗΜΑ (Closest Pair)
# ======================================================
print("\n3. [Q3] Κοντινότερο Ζευγάρι Πλοίων (15 Αυγούστου, 12:00)")
start_time = time.time()

target_time = datetime.datetime(2017, 8, 15, 12, 0, 0)
start_window = target_time - datetime.timedelta(minutes=2)
end_window = target_time + datetime.timedelta(minutes=2)

pipeline_q3 = [
    {
        "$match": {
            "bucket_period.start": { "$lte": end_window },
            "bucket_period.end": { "$gte": start_window }
        }
    },
    { "$unwind": "$points" },
    {
        "$match": {
            "points.ts": { "$gte": start_window, "$lte": end_window }
        }
    },
    {
        "$project": {
            "ship_id": 1,
            "lat": { "$arrayElemAt": ["$points.location.coordinates", 1] },
            "lon": { "$arrayElemAt": ["$points.location.coordinates", 0] }
        }
    }
]

candidates = list(collection.aggregate(pipeline_q3))
print(f"   -> Βρέθηκαν {len(candidates)} υποψήφια στίγματα.")

min_dist = float('inf')
closest_pair = None

def haversine(lon1, lat1, lon2, lat2):
    R = 6371 
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    return R * c

for i in range(len(candidates)):
    for j in range(i + 1, len(candidates)):
        p1 = candidates[i]
        p2 = candidates[j]
        if p1['ship_id'] == p2['ship_id']: continue
        dist = haversine(p1['lon'], p1['lat'], p2['lon'], p2['lat'])
        if dist < min_dist:
            min_dist = dist
            closest_pair = (p1['ship_id'], p2['ship_id'])

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
if closest_pair:
    print(f"   - Ζευγάρι: {closest_pair[0]} & {closest_pair[1]}")
    print(f"   - Απόσταση: {min_dist * 1000:.2f} μέτρα")


# ======================================================
# 4. ΓΕΩΧΩΡΙΚΟ: ΠΟΛΥΓΩΝΟ (Σαρωνικός)
# ======================================================
print("\n4. [Q4] Πλοία στον Σαρωνικό (Πολύγωνο)")
start_time = time.time()

polygon_area = {
    "type": "Polygon",
    "coordinates": [[
        [23.35, 37.85] ,[23.75, 37.85] ,[23.75, 37.60] ,[23.35, 37.60] , [23.35, 37.85]
    ]]
}

query_q4 = {
    "points.location": {
        "$geoWithin": { "$geometry": polygon_area }
    }
}

ships_in_area = collection.distinct("ship_id", query_q4)

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
print(f"   - Βρέθηκαν {len(ships_in_area)} μοναδικά πλοία.")


# ======================================================
# 5. ΓΕΩΧΩΡΙΚΟ: ΑΚΤΙΝΑ (Πειραιάς)
# ======================================================
print("\n5. [Q5] Πλοία σε ακτίνα 10km από το κέντρο του Πειραιά")
start_time = time.time()

piraeus_center = [23.643, 37.943]
radius_km = 10
radius_radians = radius_km / 6378.1

query_q5 = {
    "points.location": {
        "$geoWithin": { 
            "$centerSphere": [piraeus_center, radius_radians] 
        }
    }
}
ships_nearby = collection.distinct("ship_id", query_q5)

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
print(f"   - Βρέθηκαν {len(ships_nearby)} πλοία.")


# ======================================================
# 6. ΧΩΡΟ-ΧΡΟΝΙΚΟ (Spatio-Temporal)
# ======================================================
print("\n6. [Q6] Πλοία στον Σαρωνικό το τριήμερο του Δεκαπενταύγουστου")
start_time = time.time()

d_start = datetime.datetime(2017, 8, 14)
d_end = datetime.datetime(2017, 8, 17)

query_q6 = {
    "points.location": { "$geoWithin": { "$geometry": polygon_area } },
    "bucket_period.start": { "$gte": d_start },
    "bucket_period.end": { "$lte": d_end }
}
ships_spatiotemporal = collection.distinct("ship_id", query_q6)

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")
print(f"   - Βρέθηκαν {len(ships_spatiotemporal)} πλοία.")


# ======================================================
# 7. ΑΝΑΛΥΣΗ ΚΑΚΟΚΑΙΡΙΑΣ
# ======================================================
print("\n7. [Q7] Στίγματα με δυνατό άνεμο (> 15 m/s) & Έλεγχος Χρονικής Διαφοράς")
start_time = time.time()

pipeline_q7 = [
    { "$match": { "points.weather.wind_speed": { "$gt": 15 } } },
    { "$unwind": "$points" },
    { "$match": { "points.weather.wind_speed": { "$gt": 15 } } },
    {
        "$group": {
            "_id": None,
            "total_stormy_points": { "$sum": 1 },
            "avg_time_offset": { "$avg": "$points.weather.time_offset_min" }
        }
    }
]

res_q7 = list(collection.aggregate(pipeline_q7))

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")

if res_q7:
    count = res_q7[0]['total_stormy_points']
    avg_offset = res_q7[0]['avg_time_offset']
    print(f"   - Βρέθηκαν {count} στίγματα σε κακοκαιρία.")
    print(f"   - Μέση χρονική απόκλιση μέτρησης καιρού: {avg_offset:.2f} λεπτά")
else:
    print("   - Δεν βρέθηκαν στίγματα με τόσο δυνατό άνεμο.")


# ======================================================
# 8. ΕΛΕΓΧΟΣ ΠΟΙΟΤΗΤΑΣ (Temporal Join Check)
# ======================================================
print("\n8. [Q8] Πόσο ακριβές είναι το temporal join; (Sample 1000 docs)")
start_time = time.time()

pipeline_q8 = [
    { "$sample": { "size": 1000 } },
    { "$unwind": "$points" },
    { "$match": { "points.weather.time_offset_min": { "$exists": True } } },
    {
        "$group": {
            "_id": None,
            "avg_offset": { "$avg": "$points.weather.time_offset_min" },
            "max_offset": { "$max": "$points.weather.time_offset_min" },
            "min_offset": { "$min": "$points.weather.time_offset_min" }
        }
    }
]
res_q8 = list(collection.aggregate(pipeline_q8))
end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")

if res_q8:
    print(f"    Στατιστικά Χρονικής Απόκλισης:")
    print(f"      Μέση: {res_q8[0]['avg_offset']:.2f} λεπτά")
    print(f"      Μέγιστη: {res_q8[0]['max_offset']:.2f} λεπτά")
    print(f"      Ελάχιστη: {res_q8[0]['min_offset']:.2f} λεπτά")


# ======================================================
# 9. IMPACT ANALYSIS (Ταχύτητα vs Καιρός)
# ======================================================
print("\n9. [Q9] Επίδραση Καιρού στην Ταχύτητα (Άπνοια vs Φουρτούνα)")
start_time = time.time()

pipeline_impact = [
    { "$unwind": "$points" },
    # Φίλτρο: Αγνοούμε τα σταματημένα πλοία για να μην χαλάσουν τον μέσο όρο
    { "$match": { "points.speed": { "$gt": 1 } } },
    {
        "$facet": {
            "Calm_Weather": [
                { "$match": { "points.weather.wind_speed": { "$lt": 5 } } },
                { "$group": { "_id": None, "avg_speed": { "$avg": "$points.speed" } } }
            ],
            "Rough_Weather": [
                { "$match": { "points.weather.wind_speed": { "$gt": 8 } } },
                { "$group": { "_id": None, "avg_speed": { "$avg": "$points.speed" } } }
            ]
        }
    }
]

res_impact = list(collection.aggregate(pipeline_impact))

end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")

if res_impact:
    calm = res_impact[0]['Calm_Weather'][0]['avg_speed'] if res_impact[0]['Calm_Weather'] else 0
    rough = res_impact[0]['Rough_Weather'][0]['avg_speed'] if res_impact[0]['Rough_Weather'] else 0
    
    print(f"   - Μέση Ταχύτητα σε Άπνοια (<5):   {calm:.2f} knots")
    print(f"   - Μέση Ταχύτητα σε Φουρτούνα(>8): {rough:.2f} knots")
    
    if calm > rough:
        diff = ((calm - rough) / calm) * 100
        print(f"    Συμπέρασμα: Ταχύτητα μειωμένη κατά {diff:.1f}% λόγω καιρού.")
    else:
        print("    Συμπέρασμα: Δεν παρατηρήθηκε σημαντική διαφορά.")


# ======================================================
# 10. EXPOSURE ANALYSIS (Ποιος αντέχει τον αέρα;)
# ======================================================
print("\n10. [Q10] Μέσος Άνεμος ανά Τύπο Πλοίου")
start_time = time.time()

pipeline_exposure = [
    { "$unwind": "$points" },
    { "$match": { "points.weather.wind_speed": { "$exists": True } } },
    {
        "$group": {
            "_id": "$static_data.vessel_type",
            "avg_wind_faced": { "$avg": "$points.weather.wind_speed" },
            "max_wind_faced": { "$max": "$points.weather.wind_speed" }
        }
    },
    { "$sort": { "avg_wind_faced": -1 } },
    { "$limit": 5 }
]

res_exposure = list(collection.aggregate(pipeline_exposure))
end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")

for r in res_exposure:
    print(f"   - {r['_id']}: Μέσος {r['avg_wind_faced']:.1f} m/s (Max: {r['max_wind_faced']} m/s)")


# ======================================================
# 11. EXTREME EVENT (Καύσωνας)
# ======================================================
print("\n11. [Q11] Ρεκόρ Υψηλότερης Θερμοκρασίας")
start_time = time.time()

pipeline_heat = [
    { "$unwind": "$points" },
    { "$match": { "points.weather.temp_c": { "$exists": True } } },
    { "$sort": { "points.weather.temp_c": -1 } },
    { "$limit": 1 },
    {
        "$project": {
            "ship_id": 1,
            "vessel_type": "$static_data.vessel_type",
            "timestamp": "$points.ts",
            "temp": "$points.weather.temp_c",
            "coords": "$points.location.coordinates"
        }
    }
]

res_heat = list(collection.aggregate(pipeline_heat))
end_time = time.time()
print(f"    Χρόνος: {end_time - start_time:.4f} sec")

if res_heat:
    rec = res_heat[0]
    print(f"    Θερμοκρασία: {rec['temp']} °C")
    print(f"    Πλοίο: {rec['ship_id']} ({rec['vessel_type']})")
    print(f"    Πότε: {rec['timestamp']}")
    print(f"    Πού: {rec['coords']}")

print("\n--- ΤΕΛΟΣ ΕΚΤΕΛΕΣΗΣ ---")