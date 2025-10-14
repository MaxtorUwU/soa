# archivo_service.py
import socket
import time

SERVICE_NAME = 'ARCHI'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_upload(params):
    filename, file_data_base64 = params
    print(f"--> TODO: Almacenar el archivo {filename} en el disco.")
    # Lógica real: decodificar file_data_base64 y guardarlo en el sistema de archivos.
    return b'OK;File stored successfully'

def process_download(params):
    filename = params[0]
    print(f"--> TODO: Leer el archivo {filename} del disco y prepararlo para descarga.")
    # Lógica real: leer el archivo y codificarlo en base64 para enviarlo.
    return b'OK;file_data_in_base64'

def process_delete(params):
    filename = params[0]
    print(f"--> TODO: Eliminar el archivo {filename} del disco.")
    # Lógica real: usar os.remove() para borrar el archivo.
    return b'OK;File deleted'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode().split(';')
        transaction_name, params = decoded_data[0], decoded_data[1:]
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'upload':
            response = process_upload(params)
        elif transaction_name == 'download':
            response = process_download(params)
        elif transaction_name == 'delete':
            response = process_delete(params)
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