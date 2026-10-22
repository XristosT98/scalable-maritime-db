from pymongo import MongoClient

# Σύνδεση (πρόσεξε την πόρτα 27018)
client = MongoClient('mongodb://localhost:27018/')
db = client['MaritimeDB_Advanced_Final']
collection = db['trajectories_temporal']

# 1. Αριθμός Εγγράφων
count = collection.count_documents({})
print(f"Συνολικά Έγγραφα (Count): {count}")

# 2. Στατιστικά Βάσης (Μέγεθος)
stats = db.command("dbStats")

# Μετατροπή bytes σε MB
storage_size_mb = stats['storageSize'] / (1024 * 1024)
data_size_mb = stats['dataSize'] / (1024 * 1024)

print(f"Μέγεθος στο δίσκο (Storage Size): {storage_size_mb:.2f} MB")
print(f"Μέγεθος δεδομένων (Data Size): {data_size_mb:.2f} MB")