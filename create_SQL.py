import sqlite3
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("log.db")
cursor = conn.cursor()

# Create the table without the 'geoip_city' column
create_table_query = """
CREATE TABLE IF NOT EXISTS log (
    id INTEGER PRIMARY KEY,
    timestamp TEXT,
    method TEXT,
    path TEXT,
    status INTEGER,
    size INTEGER,
    user_agent TEXT,
    geoip_country TEXT,
    device_type TEXT,
    ip TEXT
);
"""

cursor.execute(create_table_query)

# Commit changes and close the connection
conn.commit()

# Function to create fake log data (excluding geoip_city)
def generate_detailed_logs(num_logs):
    logs = []
    current_time = datetime.utcnow()
    for _ in range(num_logs):
        ip = fake.ipv4_public()
        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
        method = random.choice(["GET", "POST", "PUT", "DELETE"])
        path = random.choice(["/home", "/about.html", "/contact.html", "/login", "/products.html", "/services.html"])
        status = random.choice([200, 301, 302, 404, 500])
        size = random.randint(200, 5000)
        user_agent = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"
        ])
        geoip_country = fake.country()
        device_type = "Desktop" if "Windows" in user_agent or "Linux" in user_agent else "Mobile" if "Mobile" in user_agent or "Android" in user_agent else "Tablet" if "iPad" in user_agent or "Macintosh" in user_agent else "Unknown"

        log = {
            "ip": ip,
            "timestamp": timestamp,
            "method": method,
            "path": path,
            "status": status,
            "size": size,
            "user_agent": user_agent,
            "geoip_country": geoip_country,
            "device_type": device_type
        }
        logs.append(log)
        current_time -= timedelta(seconds=random.randint(1, 300))
    return logs

data = generate_detailed_logs(1000)

# Insert data into the table (excluding geoip_city)
insert_query = """
INSERT INTO log (timestamp, method, path, status, size, user_agent, geoip_country, device_type, ip)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Prepare data for insertion
records = [(log['timestamp'], log['method'], log['path'], log['status'], log['size'], log['user_agent'], log['geoip_country'], log['device_type'], log['ip']) for log in data]

cursor.executemany(insert_query, records)

# Commit changes and close the connection
conn.commit()

query = "SELECT * FROM log"

# Read the SQL query into a DataFrame
df = pd.read_sql_query(query, conn)

# Close the connection
conn.close()

# Convert the 'timestamp' column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')

# Sort the DataFrame by timestamp
df = df.sort_values(by='timestamp')

# Reset index to ensure sequential ID assignment
df.reset_index(drop=True, inplace=True)

# Assign a new 'id' column based on the sorted DataFrame
df['id'] = df.index + 1  # IDs start from 1

# Reorder columns to put 'id' at the beginning
columns = ['id'] + [col for col in df.columns if col != 'id']
df = df[columns]

# Write the DataFrame to a CSV file
df.to_csv("sorted_log_data.csv", index=False)
