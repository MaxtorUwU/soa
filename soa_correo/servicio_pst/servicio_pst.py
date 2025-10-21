import socket, psycopg2, os, base64, time, sys

STORAGE_PATH = "/app/pst_files" # Usar ruta absoluta dentro del contenedor

# --- Configuración de Conexión a la Base de Datos con Reintentos ---
def get_db_connection():
    """Intenta conectarse a la BD con reintentos."""
    conn = None
    retries = 10
    while retries > 0:
        try:
            print("[PSTPR] Intentando conectar a la base de datos...")
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST")
            )
            print("[PSTPR] Conexión a la base de datos exitosa.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[PSTPR] Error al conectar a la BD: {e}")
            retries -= 1
            print(f"[PSTPR] Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    print("[PSTPR] ERROR: No se pudo conectar a la base de datos después de varios intentos.")
    return None

# --- Configuración de Conexión al ESB con Reintentos ---
def connect_to_esb():
    """Intenta conectarse al ESB con reintentos."""
    esb_host = os.getenv("ESB_HOST")
    esb_port = int(os.getenv("ESB_PORT"))
    service_name = os.getenv("SERVICE_NAME")
    
    while True:
        try:
            print(f"[PSTPR] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            register_msg = f"register;{service_name}"
            s.sendall(register_msg.encode())
            print(f"[PSTPR] Conectado y registrado en el ESB como '{service_name}'.")
            return s
            
        except ConnectionRefusedError:
            print("[PSTPR] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[PSTPR] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx, conn_pg):
    # Ojo: El buffer de socket (8192) es pequeño para archivos.
    # El ESB debería tener un mecanismo para manejar archivos grandes.
    # Asumimos que 'tx' contiene el comando y los datos (lo cual es riesgoso).
    
    op, *params = tx.split(";")
    
    try:
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
            response = "OK"
        
        elif op == "getfiles":
            user_id = params[0]
            cur.execute("SELECT nombre_archivo, fecha_importacion FROM pst_archivos WHERE id_usuario = %s", (user_id,))
            response = str(cur.fetchall())
        
        else:
            response = "NK"
        
        cur.close()
        return response

    except psycopg2.Error as e:
        print(f"[PSTPR] Error de base de datos en handle_tx: {e}")
        conn_pg.rollback()
        return f"NK;Error de base de datos: {e}"
    except Exception as e:
        print(f"[PSTPR] Error inesperado en handle_tx: {e}")
        conn_pg.rollback()
        return f"NK;Error inesperado: {e}"

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    # 1. Conectar a la Base de Datos
    conn_pg = get_db_connection()
    if conn_pg is None:
        print("[PSTPR] CRÍTICO: No se pudo conectar a la BD. Saliendo.")
        sys.exit(1)

    # 2. Conectar al ESB
    esb_socket = connect_to_esb()

    # 3. Bucle principal para escuchar al ESB
    print("[PSTPR] Escuchando mensajes del ESB...")
    while True:
        try:
            # Aumentamos el buffer para este servicio, ya que maneja archivos
            tx = esb_socket.recv(8192 * 2).decode() # Buffer más grande
            if not tx:
                print("[PSTPR] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue

            print("[PSTPR] TX (desde ESB):", tx[:80] + "...") # Log truncado
            response = handle_tx(tx, conn_pg)
            esb_socket.sendall(response.encode())
            
        except socket.error as e:
            print(f"[PSTPR] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[PSTPR] Error en bucle principal: {e}")
            try:
                esb_socket.sendall(f"NK;Error interno del servicio: {e}".encode())
            except socket.error:
                pass

if __name__ == "__main__":
    while True:
        start_service()
        print("[PSTPR] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)