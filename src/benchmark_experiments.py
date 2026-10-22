import time
import matplotlib.pyplot as plt
from pymongo import MongoClient
import datetime

# --- ΡΥΘΜΙΣΕΙΣ ΣΥΝΔΕΣΗΣ ---
client = MongoClient('mongodb://localhost:27018/')
db = client['MaritimeDB_Advanced_Final']
collection = db['trajectories_temporal']

print("--- ΕΝΑΡΞΗ ΠΕΙΡΑΜΑΤΙΚΗΣ ΑΞΙΟΛΟΓΗΣΗΣ ---\n")

# Λίστες για αποθήκευση αποτελεσμάτων
results_volume = []
results_radius = []
results_index = []

# ==============================================================================
# ΠΕΙΡΑΜΑ 1: Scalability (Όγκος Δεδομένων - Χρονικό Εύρος)
# Ερώτημα: Πόσο χρόνο θέλει για να μετρήσει πλοία σε εύρος Χ ημερών;
# ==============================================================================
print("1. Εκτέλεση Πειράματος Όγκου (Data Volume)...")
days_ranges = [1, 5, 10, 15, 20, 25]  # Μέρες προς έλεγχο
base_date = datetime.datetime(2017, 8, 1) # Ημερομηνία αρχής

for d in days_ranges:
    end_date = base_date + datetime.timedelta(days=d)
    
    start_t = time.time()
    # Query: Βρες έγγραφα σε αυτό το χρονικό εύρος
    count = collection.count_documents({
        "bucket_period.start": { "$gte": base_date, "$lt": end_date }
    })
    end_t = time.time()
    
    duration = end_t - start_t
    results_volume.append(duration)
    print(f"   - {d} Days: {duration:.4f} sec (Count: {count})")

# ==============================================================================
# ΠΕΙΡΑΜΑ 2: Selectivity (Γεωχωρική Ακτίνα)
# Ερώτημα: Πόσο χρόνο θέλει όσο μεγαλώνει η περιοχή αναζήτησης;
# ==============================================================================
print("\n2. Εκτέλεση Πειράματος Γεωχωρικής Ακτίνας (Radius)...")

radii_km = [1, 5, 10, 15] # Ακτίνες σε χιλιόμετρα
center_point = [23.6, 37.9] # Πειραιάς

for km in radii_km:
    radians = km / 6378.1 # Μετατροπή km σε radians
    
    start_t = time.time()
    # Query: GeoWithin CenterSphere
    pipeline = [
        {
            "$match": {
                "points.location": {
                    "$geoWithin": { "$centerSphere": [center_point, radians] }
                }
            }
        },
        { "$count": "total" }
    ]
    
    # --- ΤΡΟΠΟΠΟΙΗΣΗ ΕΔΩ ---
    # Εκτελούμε το query και κρατάμε το αποτέλεσμα σε λίστα
    res = list(collection.aggregate(pipeline))
    
    # Αν η λίστα δεν είναι κενή, παίρνουμε το 'total', αλλιώς 0
    count_docs = res[0]['total'] if res else 0
    # -----------------------
    
    end_t = time.time()
    
    duration = end_t - start_t
    results_radius.append(duration)
    
    # Τώρα τυπώνουμε και το Count
    print(f"   - Radius {km} km: {duration:.4f} sec (Count: {count_docs})")

# ==============================================================================
# ΠΕΙΡΑΜΑ 3: Index vs No Index (Full Scan)
# Ερώτημα: Πόση διαφορά κάνει το Index στο ship_id;
# ==============================================================================
print("\n3. Εκτέλεση Πειράματος Indexing...")
target_ship = "239466000" # Ένα τυχαίο MMSI που υπάρχει

# A. Με Index (Κανονικό)
t1 = time.time()
collection.find_one({"ship_id": target_ship})
t_with_index = time.time() - t1

# B. Χωρίς Index (Forces Collection Scan με $natural hint)
# ΠΡΟΣΟΧΗ: Αυτό θα αργήσει αν η βάση είναι τεράστια!
t2 = time.time()
collection.find_one({"ship_id": target_ship}, hint={ "$natural": 1 })
t_without_index = time.time() - t2

results_index = [t_with_index, t_without_index]
print(f"   - With Index: {t_with_index:.6f} sec")
print(f"   - Without Index: {t_without_index:.6f} sec")
print(f"   -> Speedup: {t_without_index / t_with_index:.1f}x faster")

# ==============================================================================
# ΠΑΡΑΓΩΓΗ ΓΡΑΦΗΜΑΤΩΝ (PLOTTING)
# ==============================================================================
print("\n--> Δημιουργία Γραφημάτων (benchmarks.png)...")

plt.figure(figsize=(15, 5))

# Plot 1: Volume
plt.subplot(1, 3, 1)
plt.plot(days_ranges, results_volume, marker='o', color='b', linestyle='-')
plt.title('Performance vs Data Volume')
plt.xlabel('Time Range (Days)')
plt.ylabel('Query Time (sec)')
plt.grid(True)

# Plot 2: Radius
plt.subplot(1, 3, 2)
plt.plot(radii_km, results_radius, marker='s', color='r', linestyle='--')
plt.title('Performance vs Spatial Filter Size')
plt.xlabel('Radius (km)')
plt.ylabel('Query Time (sec)')
plt.grid(True)

# Plot 3: Indexing
plt.subplot(1, 3, 3)
labels = ['With Index', 'Without Index']
colors = ['green', 'orange']
plt.bar(labels, results_index, color=colors)
plt.title('Impact of Indexing')
plt.ylabel('Query Time (sec)')
# Επειδή η διαφορά είναι χαώδης, χρησιμοποιούμε λογαριθμική κλίμακα αν χρειαστεί
# plt.yscale('log') 

plt.tight_layout()
plt.savefig('benchmarks.png')
print("Ολοκληρώθηκε! Δες το αρχείο benchmarks.png")
plt.show()