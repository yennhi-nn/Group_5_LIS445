import os, time, shutil, csv, mysql.connector

def connect_db():
    while True:
        try:
            return mysql.connector.connect(host="mysql_db", user="root", password="root", database="webstore")
        except:
            print("Waiting for MySQL...")
            time.sleep(5)

def process():
    path = "/app/input/inventory.csv"
    if os.path.exists(path):
        conn = connect_db()
        cursor = conn.cursor()
        with open(path, 'r') as f:
            for row in csv.reader(f):
                try:
                    p_id, qty = int(row[0]), int(row[1])
                    if qty >= 0:
                        cursor.execute("UPDATE products SET quantity = %s WHERE id = %s", (qty, p_id))
                except: continue
        conn.commit()
        shutil.move(path, f"/app/processed/inventory_{int(time.time())}.csv")
        conn.close()

if __name__ == "__main__":
    while True:
        process()
        time.sleep(10)