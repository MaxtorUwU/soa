# correo_service.py
import socket
import time

SERVICE_NAME = 'CORREO'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_getemails(params):
    user_id = params[0]
    print(f"--> TODO: Implementar búsqueda de correos para usuario {user_id}")
    # Lógica real: conectar a Elasticsearch/BD y buscar correos.
    return b'OK;email1_data,email2_data'

def process_search(params):
    query, filters = params[0], params[1:]
    print(f"--> TODO: Implementar búsqueda de '{query}' con filtros {filters}")
    return b'OK;search_results_json'

def process_view(params):
    email_id = params[0]
    print(f"--> TODO: Implementar obtención de detalles para correo {email_id}")
    return b'OK;full_email_content'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode().split(';')
        transaction_name, params = decoded_data[0], decoded_data[1:]
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'getemails':
            response = process_getemails(params)
        elif transaction_name == 'search':
            response = process_search(params)
        elif transaction_name == 'view':
            response = process_view(params)
        else:
            response = b'NK;Unknown transaction'
        
        return response
    except Exception as e:
        print(f"[ERROR] {e}")
        return b'NK;Error processing request'

# --- BUCLE PRINCIPAL DEL SERVICIO ---
if __name__ == "__main__":
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(BUS_ADDRESS)
            registration_msg = f'sinit{SERVICE_NAME}'.encode()
            message = f'{len(registration_msg):05d}'.encode() + registration_msg
            sock.sendall(message)
            print(f"[{SERVICE_NAME}] Servicio registrado. Listo para recibir peticiones.")
            while True:
                header = sock.recv(5)
                if not header: break
                data = sock.recv(int(header))
                response = handle_transaction(data)
                sock.sendall(f'{len(response):05d}'.encode() + response)
        except (ConnectionRefusedError, BrokenPipeError):
            print(f"[{SERVICE_NAME}] Conexión perdida. Reintentando en 5 segundos...")
            time.sleep(5)
        finally:
            sock.close()