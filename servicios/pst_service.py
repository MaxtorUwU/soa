# pst_service.py
import socket
import time

SERVICE_NAME = 'PSTPR'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_upload(params):
    user_id, filename = params
    print(f"--> TODO: Iniciar procesamiento asíncrono del archivo {filename} para usuario {user_id}")
    # Lógica real: encolar una tarea pesada (usando Celery, RQ, etc.).
    return b'OK;Processing job started'

def process_getfiles(params):
    user_id = params[0]
    print(f"--> TODO: Listar archivos PST procesados para usuario {user_id}")
    return b'OK;file1.pst,file2.pst'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode().split(';')
        transaction_name, params = decoded_data[0], decoded_data[1:]
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'upload':
            response = process_upload(params)
        elif transaction_name == 'getfiles':
            response = process_getfiles(params)
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