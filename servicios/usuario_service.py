# usuario_service.py
import socket
import time

SERVICE_NAME = 'USUARIO'
BUS_ADDRESS = ('localhost', 5000)

# --- LÓGICA DE NEGOCIO ---
def process_login(params):
    email, password = params
    print(f"--> TODO: Implementar login para: {email}")
    # Lógica real: conectar a BD, verificar credenciales, generar token.
    return b'OK;login_successful_token'

def process_register(params):
    nombre, email, password, rol = params
    print(f"--> TODO: Implementar registro para: {nombre} ({email})")
    # Lógica real: validar datos, hashear contraseña, insertar en BD.
    return b'OK;user_registered_successfully'

def process_update(params):
    user_id, user_data = params
    print(f"--> TODO: Implementar actualización para usuario {user_id}")
    return b'OK;user_updated'
    
def process_delete(params):
    user_id = params[0]
    print(f"--> TODO: Implementar eliminación del usuario {user_id}")
    return b'OK;user_deleted'

# --- MANEJADOR DE TRANSACCIONES (Despachador) ---
def handle_transaction(data):
    try:
        decoded_data = data.decode()
        parts = decoded_data.split(';')
        transaction_name = parts[0]
        params = parts[1:]
        
        print(f"[{SERVICE_NAME}] Petición para transacción: '{transaction_name}'")

        if transaction_name == 'login':
            response = process_login(params)
        elif transaction_name == 'regis':
            response = process_register(params)
        elif transaction_name == 'updat':
            response = process_update(params)
        elif transaction_name == 'delet':
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