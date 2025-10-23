import socket, psycopg2, os, time, sys

# --- Configuración de Conexión a la Base de Datos con Reintentos ---
def get_db_connection():
    """Intenta conectarse a la BD con reintentos."""
    conn = None
    retries = 10
    while retries > 0:
        try:
            print("[USUAR] Intentando conectar a la base de datos...")
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST")
            )
            print("[USUAR] Conexión a la base de datos exitosa.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[USUAR] Error al conectar a la BD: {e}")
            retries -= 1
            print(f"[USUAR] Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    print("[USUAR] ERROR: No se pudo conectar a la base de datos después de varios intentos.")
    return None

# --- Configuración de Conexión al ESB con Reintentos ---
def connect_to_esb():
    """Intenta conectarse al ESB con reintentos."""
    esb_host = os.getenv("ESB_HOST")
    esb_port = int(os.getenv("ESB_PORT"))
    service_name = os.getenv("SERVICE_NAME") # Ej: "USUAR"
    
    while True:
        try:
            print(f"[USUAR] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            command = "sinit" # 5 chars
            payload = command + service_name # "sinit" + "USUAR" = "sinitUSUAR" (10 chars)
            length_prefix = f"{len(payload):05d}" # "00010"
            register_msg = length_prefix + payload # "00010sinitUSUAR"
            
            s.sendall(register_msg.encode())
            print(f"[USUAR] Conectado y registrado en el ESB como '{service_name}' (Msg: {register_msg}).")
            return s
            
        except ConnectionRefusedError:
            print("[USUAR] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[USUAR] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx, conn_pg):
    # tx llega como "USUARregis;..." o "USUARlogin;..."
    tx = tx[5:] # <-- ¡FIX APLICADO AQUÍ! (Ignora los 5 primeros caracteres)
    # ahora tx es "regis;..." o "login;..."
    
    op, *params = tx.split(";")
    
    try:
        cur = conn_pg.cursor()
        
        if op == "login":
            email, pw = params
            cur.execute("SELECT * FROM usuarios WHERE email=%s AND contrasena=%s", (email, pw))
            response = "OK" if cur.fetchone() else "NK"
        
        elif op == "regis":
            nombre, email, pw, rol = params
            cur.execute("INSERT INTO usuarios (nombre,email,contrasena,rol) VALUES (%s,%s,%s,%s)",
                        (nombre, email, pw, rol == "True"))
            conn_pg.commit()
            response = "OK"
        
        else:
            response = None 
        
        cur.close()
        return response

    except psycopg2.Error as e:
        print(f"[USUAR] Error de base de datos en handle_tx: {e}")
        conn_pg.rollback()
        return "NK" # Devolver NK en caso de error de BD
    except Exception as e:
        print(f"[USUAR] Error inesperado en handle_tx ({tx}): {e}")
        conn_pg.rollback()
        return "NK" # Devolver NK en caso de error de código

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    conn_pg = get_db_connection()
    if conn_pg is None:
        sys.exit(1)

    esb_socket = connect_to_esb()
    print("[USUAR] Escuchando mensajes del ESB...")
    
    while True:
        try:
            tx_with_prefix = esb_socket.recv(4096).decode()
            if not tx_with_prefix:
                print("[USUAR] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue
            
            if len(tx_with_prefix) < 5:
                print(f"[USUAR] Ignorando mensaje corrupto (muy corto): {tx_with_prefix}")
                continue
                
            tx_payload = tx_with_prefix[5:] # Quitar los 5 dígitos del prefijo
            print(f"[USUAR] TX (desde ESB): {tx_payload}")
            
            # Pasamos el tx_payload que SÍ incluye el prefijo del servicio
            response = handle_tx(tx_payload, conn_pg) 
            
            if response is not None:
                response_tx = f"{len(response):05d}{response}"
                esb_socket.sendall(response_tx.encode())
            else:
                print(f"[USUAR] Ignorando TX (sin respuesta): {tx_payload}")
            
        except socket.error as e:
            print(f"[USUAR] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[USUAR] Error en bucle principal: {e}")

if __name__ == "__main__":
    while True:
        start_service()
        print("[USUAR] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)