import socket
import psycopg2
import os

# Database connection
conn_pg = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST")
)

def handle_tx(tx):
    """
    Handles incoming transactions for the notification service.
    """
    op, *params = tx.split(";")
    cur = conn_pg.cursor()

    if op == "sendalert":
        # Transaction: sendalert;id_usuario;mensaje;tipo
        id_usuario, mensaje, tipo = params
        cur.execute(
            "INSERT INTO notificaciones (id_usuario, mensaje, tipo) VALUES (%s, %s, %s)",
            (id_usuario, mensaje, tipo)
        )
        conn_pg.commit()
        return "OK: Notificacion creada"

    elif op == "getnotif":
        # Transaction: getnotif;id_usuario
        id_usuario = params[0]
        cur.execute(
            "SELECT id, mensaje, tipo, fecha_envio, leido FROM notificaciones WHERE id_usuario = %s ORDER BY fecha_envio DESC",
            (id_usuario,)
        )
        notifications = cur.fetchall()
        return str(notifications)

    return "NK: Operacion desconocida"

def start_service():
    """
    Starts the notification service server.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5500))
    s.listen()
    print("[NOTIF] Activo en puerto 5500, esperando conexiones...")

    while True:
        conn, _ = s.accept()
        try:
            tx = conn.recv(4096).decode()
            if tx:
                print(f"[NOTIF] TX: {tx}")
                response = handle_tx(tx)
                conn.sendall(response.encode())
        except Exception as e:
            print(f"Error handling connection: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    start_service()
