# cliente_principal.py
import socket
import sys

# --- Configuración del Cliente ---
BUS_ADDRESS = ('localhost', 5000)

def send_transaction(sock, service_name, data):
    """
    Construye el mensaje con el formato requerido (NNNNN<servicio><datos>),
    lo envía y espera la respuesta.
    """
    try:
        # Formato: <longitud_total_5_digitos><nombre_servicio><datos>
        message_body = f"{service_name}{data}".encode()
        message = f'{len(message_body):05d}'.encode() + message_body
        
        print(f"\n[CLIENTE] -> Enviando a '{service_name}': {data}")
        sock.sendall(message)
        
        # Esperar la respuesta del servicio
        header = sock.recv(5)
        if not header:
            print("[CLIENTE] <- No se recibió respuesta del bus.")
            return None
        
        amount_expected = int(header)
        response = sock.recv(amount_expected)
        
        print(f"[CLIENTE] <- Respuesta recibida: {response.decode()}")
        return response.decode()
        
    except (ConnectionRefusedError, BrokenPipeError):
        print("[ERROR] No se pudo conectar al bus. Asegúrate de que esté corriendo.")
        return None
    except Exception as e:
        print(f"[ERROR] Ocurrió un error inesperado: {e}")
        return None

def main_menu():
    """Muestra el menú principal y maneja la lógica de la aplicación."""
    
    # Conectarse al bus una sola vez
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(BUS_ADDRESS)
    except ConnectionRefusedError:
        print("[ERROR] No se pudo conectar al bus. Finalizando programa.")
        return

    while True:
        print("\n--- MENÚ PRINCIPAL DEL SISTEMA ---")
        print("1. Iniciar Sesión (USUARIO)")
        print("2. Registrar Usuario (USUARIO)")
        print("3. Buscar Correos (CORREO)")
        print("4. Subir archivo PST (PSTPR)")
        print("5. Enviar un Log (LOGAU)")
        print("6. Enviar Notificación (NOTIF)")
        print("0. Salir")
        
        choice = input("Seleccione una opción: ")
        
        if choice == '1':
            email = input("  Email: ")
            password = input("  Contraseña: ")
            send_transaction(sock, 'USUARIO', f'login;{email};{password}')
        
        elif choice == '2':
            nombre = input("  Nombre: ")
            email = input("  Email: ")
            password = input("  Contraseña: ")
            send_transaction(sock, 'USUARIO', f'regis;{nombre};{email};{password};user')

        elif choice == '3':
            user_id = input("  ID de usuario para buscar correos: ")
            send_transaction(sock, 'CORREO', f'getemails;{user_id}')
            
        elif choice == '4':
            user_id = input("  ID de usuario que sube el archivo: ")
            filename = input("  Nombre del archivo PST: ")
            send_transaction(sock, 'PSTPR', f'upload;{user_id};{filename}')
            
        elif choice == '5':
            user = input("  Usuario que genera el log: ")
            desc = input("  Descripción del evento: ")
            send_transaction(sock, 'LOGAU', f'logevent;INFO;{user};{desc}')

        elif choice == '6':
            user_id = input("  ID de usuario a notificar: ")
            msg = input("  Mensaje de la notificación: ")
            send_transaction(sock, 'NOTIF', f'sendalert;{user_id};{msg}')

        elif choice == '0':
            break
        else:
            print("Opción no válida. Intente de nuevo.")
            
    # Cerrar la conexión al salir
    print("Cerrando conexión.")
    sock.close()


if __name__ == "__main__":
    main_menu()