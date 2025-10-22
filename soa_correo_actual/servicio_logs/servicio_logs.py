import socket, psycopg2, os

conn_pg = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST")
)

def handle_tx(tx):
    op, *params = tx.split(";")
    cur = conn_pg.cursor()
    if op == "addlog":
        id_usuario, accion, descripcion, nivel = params
        cur.execute("INSERT INTO logs (id_usuario, accion, descripcion, nivel) VALUES (%s,%s,%s,%s)",
                    (id_usuario, accion, descripcion, nivel))
        conn_pg.commit()
        return "OK"
    elif op == "getlogs":
        cur.execute("SELECT id, accion, nivel FROM logs ORDER BY fecha_evento DESC LIMIT 5")
        return str(cur.fetchall())
    return "NK"

def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5400))
    s.listen()
    print("[LOGAU] Activo en puerto 5400, esperando conexiones...")
    while True:
        conn, _ = s.accept()
        tx = conn.recv(4096).decode()
        print("[LOGAU] TX:", tx)
        response = handle_tx(tx)
        conn.sendall(response.encode())
        conn.close()

if __name__ == "__main__":
    start_service()
