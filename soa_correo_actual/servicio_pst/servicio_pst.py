import socket, psycopg2, os, base64

conn_pg = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST")
)

STORAGE_PATH = "pst_files"

def handle_tx(tx):
    op, *params = tx.split(";")
    cur = conn_pg.cursor()
    if op == "upload":
        file_data, user_id, filename = params
        file_bytes = base64.b64decode(file_data.encode())
        os.makedirs(STORAGE_PATH, exist_ok=True)
        file_path = f"{STORAGE_PATH}/{filename}"
        with open(file_path, "wb") as f:
            f.write(file_bytes)
        cur.execute("INSERT INTO pst_archivos (id_usuario, nombre_archivo, ruta_archivo) VALUES (%s,%s,%s)",
                    (user_id, filename, file_path))
        conn_pg.commit()
        return "OK"
    elif op == "getfiles":
        user_id = params[0]
        cur.execute("SELECT nombre_archivo, fecha_importacion FROM pst_archivos WHERE id_usuario = %s", (user_id,))
        return str(cur.fetchall())
    return "NK"

def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5300))
    s.listen()
    print("[PSTPR] Activo en puerto 5300, esperando conexiones...")
    while True:
        conn, _ = s.accept()
        tx = conn.recv(8192).decode()
        print("[PSTPR] TX:", tx[:80])
        response = handle_tx(tx)
        conn.sendall(response.encode())
        conn.close()

if __name__ == "__main__":
    start_service()
