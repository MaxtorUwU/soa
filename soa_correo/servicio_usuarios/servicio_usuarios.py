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
    if op == "login":
        email, pw = params
        cur.execute("SELECT * FROM usuarios WHERE email=%s AND contrasena=%s", (email, pw))
        return "OK" if cur.fetchone() else "NK"
    elif op == "regis":
        nombre, email, pw, rol = params
        cur.execute("INSERT INTO usuarios (nombre,email,contrasena,rol) VALUES (%s,%s,%s,%s)",
                    (nombre, email, pw, rol == "True"))
        conn_pg.commit()
        return "OK"
    return "NK"

def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5100))
    s.listen()
    print("[USUAR] Activo en puerto 5100, esperando conexiones...")
    while True:
        conn, _ = s.accept()
        tx = conn.recv(4096).decode()
        print("[USUAR] TX:", tx)
        response = handle_tx(tx)
        conn.sendall(response.encode())
        conn.close()

if __name__ == "__main__":
    start_service()
