import socket, requests, os, time, sys

# --- Configuración de Conexión a Elasticsearch con Reintentos ---
def check_elasticsearch_connection():
    """Verifica que Elasticsearch esté listo con reintentos."""
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
    service_name = os.getenv("SERVICE_NAME") # Ej: "CORRE"
    
    while True:
        try:
            print(f"[CORRE] Intentando conectar al ESB en {esb_host}:{esb_port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((esb_host, esb_port))
            
            command = "sinit" # 5 chars
            payload = command + service_name # "sinit" + "CORRE" = "sinitCORRE" (10 chars)
            length_prefix = f"{len(payload):05d}" # "00010"
            register_msg = length_prefix + payload # "00010sinitCORRE"

            s.sendall(register_msg.encode())
            print(f"[CORRE] Conectado y registrado en el ESB como '{service_name}' (Msg: {register_msg}).")
            return s
            
        except ConnectionRefusedError:
            print("[CORRE] No se pudo conectar al ESB. Reintentando en 10 segundos...")
            time.sleep(10)
        except Exception as e:
            print(f"[CORRE] Error al conectar al ESB: {e}")
            time.sleep(10)

# --- Manejador de Transacciones ---
def handle_tx(tx):
    # tx llega como "CORREsearch;..."
    tx = tx[5:] # <-- ¡FIX APLICADO AQUÍ!
    # ahora tx es "search;..."

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
        
        else:
            return None

    except requests.RequestException as e:
        print(f"[CORRE] Error al consultar Elasticsearch: {e}")
        return "NK"
    except Exception as e:
        print(f"[CORRE] Error inesperado en handle_tx ({tx}): {e}")
        return "NK"

# --- Servicio Principal (Cliente ESB) ---
def start_service():
    if not check_elasticsearch_connection():
        sys.exit(1)

    esb_socket = connect_to_esb()
    print("[CORRE] Escuchando mensajes del ESB...")
    
    while True:
        try:
            tx_with_prefix = esb_socket.recv(4096).decode()
            if not tx_with_prefix:
                print("[CORRE] Conexión con ESB perdida. Intentando reconectar...")
                esb_socket.close()
                esb_socket = connect_to_esb()
                continue
            
            if len(tx_with_prefix) < 5:
                print(f"[CORRE] Ignorando mensaje corrupto (muy corto): {tx_with_prefix}")
                continue
                
            tx_payload = tx_with_prefix[5:] # Quitar los 5 dígitos del prefijo
            print(f"[CORRE] TX (desde ESB): {tx_payload}")
            
            response = handle_tx(tx_payload)
            
            if response is not None:
                response_tx = f"{len(response):05d}{response}"
                esb_socket.sendall(response_tx.encode())
            else:
                print(f"[CORRE] Ignorando TX (sin respuesta): {tx_payload}")
            
        except socket.error as e:
            print(f"[CORRE] Error de socket: {e}. Intentando reconectar...")
            esb_socket.close()
            esb_socket = connect_to_esb()
        except Exception as e:
            print(f"[CORRE] Error en bucle principal: {e}")

if __name__ == "__main__":
    while True:
        start_service()
        print("[CORRE] El servicio principal se detuvo, reiniciando en 10 segundos...")
        time.sleep(10)