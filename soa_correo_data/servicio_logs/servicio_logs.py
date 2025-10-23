import socket, psycopg2, os, time, sys

# --- Configuración de Conexión a la Base de Datos con Reintentos ---
def get_db_connection():
    """Intenta conectarse a la BD con reintentos."""
    conn = None
    retries = 10
    while retries > 0:
        try:
            print("[LOGAU] Intentando conectar a la base de datos...")
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST")
            )
            print("[LOGAU] Conexión a la base de datos exitosa.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[LOGAU] Error al conectar a la BD: {e}")
            retries -= 1
            print(f"[LOGAU] Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    print("[LOGAU] ERROR: No se pudo conectar a la base de datos después de varios intentos.")
    return None

# --- Configuración de Conexión al ESB con Reintentos ---
def connect_to_esb():
    """Intenta conectarse al ESB con reintentos."""
    esb_host = os.getenv("ESB_HOST")
    esb_port = int(os.getenv("ESB_PORT"))
    service_name = os.getenv("SERVICE_NAME") # Ej: "LOGAU"
    
    while True:
        try:
            print(f"[LOGAU] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            command = "sinit" # 5 chars
            payload = command + service_name # "sinit" + "LOGAU" = "sinitLOGAU" (10 chars)
            length_prefix = f"{len(payload):05d}" # "00010"
            register_msg = length_prefix + payload # "00010sinitLOGAU"

            s.sendall(register_msg.encode())
            print(f"[LOGAU] Conectado y registrado en el ESB como '{service_name}' (Msg: {register_msg}).")
            return s
            
        except ConnectionRefusedError:
            print("[LOGAU] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[LOGAU] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx, conn_pg):
    # tx llega como "LOGAUaddlog;..."
    tx = tx[5:] # <-- ¡FIX APLICADO AQUÍ!
    # ahora tx es "addlog;..."

    op, *params = tx.split(";")
    
    try:
        cur = conn_pg.cursor()
        
        if op == "addlog":
            id_usuario, accion, descripcion, nivel = params
            cur.execute("INSERT INTO logs (id_usuario, accion, descripcion, nivel) VALUES (%s,%s,%s,%s)",
                        (id_usuario, accion, descripcion, nivel))
            conn_pg.commit()
            response = "OK"
        
        elif op == "getlogs":
            cur.execute("SELECT id, accion, nivel FROM logs ORDER BY fecha_evento DESC LIMIT 5")
            response = str(cur.fetchall())
        
        else:
            response = None
        
        cur.close()
        return response

    except psycopg2.Error as e:
        print(f"[LOGAU] Error de base de datos en handle_tx: {e}")
        conn_pg.rollback()
        return "NK"
    except Exception as e:
        print(f"[LOGAU] Error inesperado en handle_tx ({tx}): {e}")
        conn_pg.rollback()
        return "NK"

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    conn_pg = get_db_connection()
    if conn_pg is None:
        sys.exit(1)

    esb_socket = connect_to_esb()
    print("[LOGAU] Escuchando mensajes del ESB...")
    
    while True:
        try:
            tx_with_prefix = esb_socket.recv(4096).decode()
            if not tx_with_prefix:
                print("[LOGAU] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue

            if len(tx_with_prefix) < 5:
                print(f"[LOGAU] Ignorando mensaje corrupto (muy corto): {tx_with_prefix}")
                continue
                
            tx_payload = tx_with_prefix[5:] # Quitar los 5 dígitos del prefijo
            print(f"[LOGAU] TX (desde ESB): {tx_payload}")
            
            response = handle_tx(tx_payload, conn_pg)
            
            if response is not None:
                response_tx = f"{len(response):05d}{response}"
                esb_socket.sendall(response_tx.encode())
            else:
                print(f"[LOGAU] Ignorando TX (sin respuesta): {tx_payload}")
            
        except socket.error as e:
            print(f"[LOGAU] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[LOGAU] Error en bucle principal: {e}")

if __name__ == "__main__":
    while True:
        start_service()
        print("[LOGAU] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)