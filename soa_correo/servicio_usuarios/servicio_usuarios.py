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
    service_name = os.getenv("SERVICE_NAME")
    
    while True:
        try:
            print(f"[USUAR] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            # 1. Registrar el servicio en el ESB
            register_msg = f"register;{service_name}"
            s.sendall(register_msg.encode())
            print(f"[USUAR] Conectado y registrado en el ESB como '{service_name}'.")
            return s
            
        except ConnectionRefusedError:
            print("[USUAR] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[USUAR] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx, conn_pg):
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
            response = "NK"
        
        cur.close()
        return response

    except psycopg2.Error as e:
        print(f"[USUAR] Error de base de datos en handle_tx: {e}")
        conn_pg.rollback() # Deshacer cambios si algo falla
        return f"NK;Error de base de datos: {e}"
    except Exception as e:
        print(f"[USUAR] Error inesperado en handle_tx: {e}")
        conn_pg.rollback()
        return f"NK;Error inesperado: {e}"

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    # 1. Conectar a la Base de Datos
    conn_pg = get_db_connection()
    if conn_pg is None:
        print("[USUAR] CRÍTICO: No se pudo conectar a la BD. Saliendo.")
        sys.exit(1) # Termina el contenedor si no hay BD

    # 2. Conectar al ESB
    esb_socket = connect_to_esb()

    # 3. Bucle principal para escuchar al ESB
    print("[USUAR] Escuchando mensajes del ESB...")
    while True:
        try:
            tx = esb_socket.recv(4096).decode()
            if not tx:
                # Conexión perdida
                print("[USUAR] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue

            print("[USUAR] TX (desde ESB):", tx)
            
            # Procesar la transacción
            response = handle_tx(tx, conn_pg)
            
            # Enviar respuesta de vuelta al ESB
            esb_socket.sendall(response.encode())
            
        except socket.error as e:
            print(f"[USUAR] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[USUAR] Error en bucle principal: {e}")
            # Enviar un error genérico si es posible
            try:
                esb_socket.sendall(f"NK;Error interno del servicio: {e}".encode())
            except socket.error:
                pass # El socket ya está roto

if __name__ == "__main__":
    while True:
        start_service()
        # Si start_service falla (ej. por pérdida de conexión), esperamos y reiniciamos
        print("[USUAR] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)