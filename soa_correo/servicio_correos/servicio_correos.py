import socket, requests, os, time, sys

# --- Configuración de Conexión a Elasticsearch con Reintentos ---
def check_elasticsearch_connection():
    """Verifica que Elasticsearch esté listo con reintentos."""
    # Usamos la URL base, no la de búsqueda
    es_base_url = os.getenv("ES_HOST") 
    retries = 10
    while retries > 0:
        try:
            print("[CORRE] Intentando conectar a Elasticsearch...")
            response = requests.get(es_base_url)
            if response.status_code == 200:
                print("[CORRE] Conexión a Elasticsearch exitosa.")
                return True
            else:
                print(f"[CORRE] Elasticsearch respondió con {response.status_code}")
        except requests.ConnectionError as e:
            print(f"[CORRE] Error al conectar a Elasticsearch: {e}")
        
        retries -= 1
        print(f"[CORRE] Reintentando en 5 segundos... ({retries} intentos restantes)")
        time.sleep(5)
    
    print("[CORRE] ERROR: No se pudo conectar a Elasticsearch después de varios intentos.")
    return False

# --- Configuración de Conexión al ESB con Reintentos ---
def connect_to_esb():
    """Intenta conectarse al ESB con reintentos."""
    esb_host = os.getenv("ESB_HOST")
    esb_port = int(os.getenv("ESB_PORT"))
    service_name = os.getenv("SERVICE_NAME")
    
    while True:
        try:
            print(f"[CORRE] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            register_msg = f"register;{service_name}"
            s.sendall(register_msg.encode())
            print(f"[CORRE] Conectado y registrado en el ESB como '{service_name}'.")
            return s
            
        except ConnectionRefusedError:
            print("[CORRE] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[CORRE] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx):
    # La URL de búsqueda se construye aquí
    es_search_url = os.getenv("ES_HOST") + "/emails/_search"
    op, *params = tx.split(";")
    
    try:
        if op == "search":
            query = params[0]
            body = {
                "query": {"multi_match": {
                    "query": query,
                    "fields": ["remitente", "destinatario", "asunto", "contenido"]
                }}
            }
            res = requests.get(es_search_url, json=body).json()
            return str(res.get("hits", {}).get("hits", []))[:300]
        
        return "NK"

    except requests.RequestException as e:
        print(f"[CORRE] Error al consultar Elasticsearch: {e}")
        return f"NK;Error de Elasticsearch: {e}"
    except Exception as e:
        print(f"[CORRE] Error inesperado en handle_tx: {e}")
        return f"NK;Error inesperado: {e}"

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    # 1. Conectar a Elasticsearch
    if not check_elasticsearch_connection():
        print("[CORRE] CRÍTICO: No se pudo conectar a Elasticsearch. Saliendo.")
        sys.exit(1)

    # 2. Conectar al ESB
    esb_socket = connect_to_esb()

    # 3. Bucle principal para escuchar al ESB
    print("[CORRE] Escuchando mensajes del ESB...")
    while True:
        try:
            tx = esb_socket.recv(4096).decode()
            if not tx:
                print("[CORRE] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue

            print("[CORRE] TX (desde ESB):", tx)
            response = handle_tx(tx)
            esb_socket.sendall(response.encode())
            
        except socket.error as e:
            print(f"[CORRE] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[CORRE] Error en bucle principal: {e}")
            try:
                esb_socket.sendall(f"NK;Error interno del servicio: {e}".encode())
            except socket.error:
                pass

if __name__ == "__main__":
    while True:
        start_service()
        print("[CORRE] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)