import sqlite3

# SQLite veritabanına bağlanın
conn = sqlite3.connect("log.db")
cursor = conn.cursor()

# Verileri sorgulayın
cursor.execute("SELECT * FROM log WHERE status = 404;")
rows = cursor.fetchall()

# Verileri yazdırın

for row in rows:
    print(row)

conn.close()
