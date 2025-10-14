# log_service.py
import socket
import time
from datetime import datetime

SERVICE_NAME = 'LOGAU'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_logevent(params):
    log_level, user, description = params
    log_entry = f"{datetime.now()} - [{log_level.upper()}] - User: {user} - Event: {description}\n"
    print(f"--> TODO: Guardar la siguiente entrada en el sistema de logs: {log_entry.strip()}")
    # Lógica real: escribir en un archivo o en una base de datos de logs (ej: ELK stack).
    with open("system_audit.log", "a") as log_file:
        log_file.write(log_entry)
    return b'OK;Event logged'

def process_getlogs(params):
    filters = params
    print(f"--> TODO: Obtener logs del sistema con los filtros: {filters}")
    # Lógica real: consultar el sistema de logs y devolver los resultados.
    return b'OK;log_content_results'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode().split(';')
        transaction_name, params = decoded_data[0], decoded_data[1:]
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'logevent':
            response = process_logevent(params)
        elif transaction_name == 'getlogs':
            response = process_getlogs(params)
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