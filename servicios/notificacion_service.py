# notificacion_service.py
import socket
import time

SERVICE_NAME = 'NOTIF'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_sendalert(params):
    user_id, message = params
    print(f"--> TODO: Enviar notificación al usuario {user_id}: '{message}'")
    # Lógica real: encolar la notificación en un sistema de mensajería (RabbitMQ, etc.).
    return b'OK;Notification queued'

def process_getnotif(params):
    user_id = params[0]
    print(f"--> TODO: Obtener notificaciones para el usuario {user_id}")
    return b'OK;notification1,notification2'

def process_markread(params):
    notification_id = params[0]
    print(f"--> TODO: Marcar la notificación {notification_id} como leída.")
    return b'OK;Marked as read'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode().split(';')
        transaction_name, params = decoded_data[0], decoded_data[1:]
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'sendalert':
            response = process_sendalert(params)
        elif transaction_name == 'getnotif':
            response = process_getnotif(params)
        elif transaction_name == 'markread':
            response = process_markread(params)
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