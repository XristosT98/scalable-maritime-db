from pymongo import MongoClient
import time

# Σύνδεση στη V2 βάση (Port 27018)
client = MongoClient('mongodb://localhost:27018/')
db = client['MaritimeDB_Advanced_Final']
collection = db['trajectories_temporal']

print("---  START COMPLEX QUERY: TORTUOSITY INDEX (V3 - PERFECT SORT) ---")
print("  Σημείωση: Αυτή η εκδοχή είναι πιο 'βαριά' γιατί ταξινομεί ΟΛΑ τα δεδομένα πριν την ομαδοποίηση.")
start_time = time.time()

pipeline = [
    # 1. MATCH: Κρατάμε buckets με αέρα (> 10 m/s) και επαρκή αριθμό στιγμάτων (> 10)
    {
        "$match": {
            "points.weather.wind_speed": { "$gt": 10 },
            "point_count": { "$gt": 10 }
        }
    },
    # 2. ADD FIELDS: Υπολογισμός με Server-Side JavaScript ($function)
    {
        "$addFields": {
            "metrics": {
                "$function": {
                    "body": """function(points) {
                        // Συνάρτηση Haversine για απόσταση σε χιλιόμετρα
                        function getDist(lon1, lat1, lon2, lat2) {
                            var R = 6371; 
                            var dLat = (lat2 - lat1) * Math.PI / 180;
                            var dLon = (lon2 - lon1) * Math.PI / 180;
                            var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                                    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                                    Math.sin(dLon/2) * Math.sin(dLon/2);
                            var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
                            return R * c;
                        }

                        if (!points || points.length < 2) return { total_dist: 0, displacement: 0, tortuosity: 1, avg_wind: 0 };

                        var total_dist = 0;
                        for (var i = 0; i < points.length - 1; i++) {
                            var p1 = points[i].location.coordinates;
                            var p2 = points[i+1].location.coordinates;
                            total_dist += getDist(p1[0], p1[1], p2[0], p2[1]);
                        }

                        var start = points[0].location.coordinates;
                        var end = points[points.length - 1].location.coordinates;
                        var displacement = getDist(start[0], start[1], end[0], end[1]);

                        // Tortuosity Index: Αν η μετατόπιση είναι > 100 μέτρα, κάνουμε τη διαίρεση
                        var ratio = (displacement > 0.1) ? (total_dist / displacement) : 1;
                        
                        // Υπολογισμός μέσου ανέμου
                        var wind_sum = 0;
                        var wind_count = 0;
                        for (var i = 0; i < points.length; i++) {
                             if (points[i].weather && points[i].weather.wind_speed) {
                                 wind_sum += points[i].weather.wind_speed;
                                 wind_count++;
                             }
                        }
                        var avg_wind = (wind_count > 0) ? (wind_sum / wind_count) : 0;

                        return { 
                            "total_km": total_dist, 
                            "displacement_km": displacement, 
                            "tortuosity": ratio,
                            "avg_wind": avg_wind
                        };
                    }""",
                    "args": ["$points"],
                    "lang": "js"
                }
            }
        }
    },
    # 3. MATCH: Φιλτράρισμα αποτελεσμάτων (Logic Fix)
    {
        "$match": {
            "metrics.tortuosity": { "$gt": 1.1 },       
            "metrics.displacement_km": { "$gt": 2.0 }   
        }
    },
    
    # -----------------------------------------------------------
    # Η ΜΕΓΑΛΗ ΑΛΛΑΓΗ (V3): SORT ΠΡΙΝ ΤΟ GROUP
    # Ταξινομούμε όλα τα buckets βάσει Tortuosity φθινουσα (-1)
    # Έτσι το "πρώτο" bucket που θα δει το Group θα είναι το χειρότερο.
    # -----------------------------------------------------------
    { "$sort": { "metrics.tortuosity": -1 } },

    # 4. GROUP: Ομαδοποίηση ανά πλοίο
    {
        "$group": {
            "_id": "$ship_id",
            "vessel_type": { "$first": "$static_data.vessel_type" },
            # Τώρα χρησιμοποιούμε $first παντού, γιατί τα δεδομένα είναι ήδη ταξινομημένα
            "max_tortuosity": { "$first": "$metrics.tortuosity" },
            "data": { "$first": "$$ROOT" } 
        }
    },
    
    # 5. FINAL SORT & LIMIT: Ταξινόμηση των τελικών πλοίων
    { "$sort": { "max_tortuosity": -1 } },
    { "$limit": 5 },
    
    # 6. PROJECT: Διαμόρφωση τελικής εξόδου
    {
        "$project": {
            "_id": 0,
            "ship_id": "$_id",
            "vessel_type": 1,
            "time_start": "$data.bucket_period.start",
            "tortuosity_index": "$max_tortuosity",
            "actual_distance": "$data.metrics.total_km",
            "straight_line": "$data.metrics.displacement_km",
            "avg_wind": "$data.metrics.avg_wind"
        }
    }
]

results = list(collection.aggregate(pipeline))
end_time = time.time()

print(f"Execution Time: {end_time - start_time:.4f} sec\n")

if results:
    for r in results:
        print(f"Ship ID: {r['ship_id']} ({r['vessel_type']})")
        print(f"   Time Start:       {r['time_start']}")
        print(f"   Avg Wind Speed:   {r['avg_wind']:.1f} m/s")
        print(f"   Actual Distance:  {r['actual_distance']:.2f} km")
        print(f"   Displacement:     {r['straight_line']:.2f} km")
        print(f"   Tortuosity Index: {r['tortuosity_index']:.2f}")
        
        # Επαλήθευση υπολογισμού
        if r['straight_line'] > 0:
            calc_check = r['actual_distance'] / r['straight_line']
            print(f"   [Check Math]:     {r['actual_distance']:.2f} / {r['straight_line']:.2f} = {calc_check:.2f}")
            
        print("-" * 60)
else:
    print("No results found.")