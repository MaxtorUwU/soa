# cliente_final_seguro.py
import socket
import sys
import os
import base64
import getpass  # Para leer contraseñas de forma segura

# --- Configuración del Cliente ---
BUS_ADDRESS = ('localhost', 5000) # Se conecta al SOABUS

def send_transaction(sock, service_name, data):
    """
    Construye el mensaje con el formato SOA, lo envía (ocultando datos sensibles
    en la consola) y espera la respuesta.
    """
    try:
        service_name_5_char = service_name.ljust(5)[:5]
        payload = f"{service_name_5_char}{data}"
        length_header = f'{len(payload):05d}'
        message = (length_header + payload).encode('utf-8')

        # --- MEJORA DE SEGURIDAD: Ocultar contraseña en el log de envío ---
        display_data = data
        if "login;" in data or "regis;" in data:
            parts = data.split(';')
            # Asumimos que la contraseña es el tercer o cuarto parámetro
            if service_name == 'USUAR' and len(parts) >= 3:
                # En 'login;email;pw', el índice es 2.
                # En 'regis;nombre;email;pw;rol', el índice es 3.
                pw_index = -2 if len(parts) == 4 else -1 # Heurística simple
                if len(parts) == 3: # login
                    parts[2] = '[******]'
                elif len(parts) == 5: # regis
                    parts[3] = '[******]'
                display_data = ';'.join(parts)
        # --- FIN DE LA MEJORA ---

        print(f"\n[CLIENTE] -> Enviando a '{service_name_5_char}': {display_data[:70]}...")
        sock.sendall(message)

        header = sock.recv(5)
        if not header:
            print("[CLIENTE] <- No se recibió respuesta del bus.")
            return None

        amount_expected = int(header.decode('utf-8'))
        response = sock.recv(amount_expected)

        # La respuesta del bus puede incluir un eco de la transacción original.
        # No podemos controlar lo que el bus devuelve, pero no lo mostramos al enviar.
        print(f"[CLIENTE] <- Respuesta recibida: {response.decode('utf-8')}")
        return response.decode('utf-8')

    except (ConnectionRefusedError, BrokenPipeError):
        print("[ERROR] No se pudo conectar al bus. Asegúrate de que esté corriendo.")
        return None
    except Exception as e:
        print(f"[ERROR] Ocurrió un error inesperado: {e}")
        return None

def main_menu():
    """Muestra el menú principal y maneja la lógica de la aplicación."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(BUS_ADDRESS)
    except ConnectionRefusedError:
        print("[ERROR] No se pudo conectar al bus. Finalizando programa.")
        return

    while True:
        print("\n--- MENÚ PRINCIPAL DEL SISTEMA ---")
        print("1. Iniciar Sesión (USUAR)")
        print("2. Registrar Usuario (USUAR)")
        print("3. Buscar Correos (CORRE)")
        print("4. Subir archivo PST (PSTPR)")
        print("5. Enviar un Log (LOGAU)")
        print("6. Enviar Notificación a Usuario (NOTIF)")
        print("7. Ver mis Notificaciones (NOTIF)")
        print("0. Salir")

        choice = input("Seleccione una opción: ")

        if choice == '1':
            email = input("  Email: ")
            # --- MEJORA DE SEGURIDAD: Usamos getpass para la contraseña ---
            password = getpass.getpass("  Contraseña: ")
            send_transaction(sock, 'USUAR', f'login;{email};{password}')

        elif choice == '2':
            nombre = input("  Nombre: ")
            email = input("  Email: ")
            # --- MEJORA DE SEGURIDAD: Usamos getpass para la contraseña ---
            password = getpass.getpass("  Contraseña: ")
            send_transaction(sock, 'USUAR', f'regis;{nombre};{email};{password};False')

        elif choice == '3':
            query = input("  Texto a buscar en correos: ")
            send_transaction(sock, 'CORRE', f'search;{query}')

        elif choice == '4':
            user_id = input("  ID de usuario que sube el archivo: ")
            filepath = input("  Ruta del archivo PST (ej: ./prueba.pst): ")

            if not os.path.exists(filepath):
                print("[ERROR] El archivo no existe en esa ruta.")
                continue
            filename = os.path.basename(filepath)
            try:
                with open(filepath, "rb") as f: file_bytes = f.read()
                file_data_base64 = base64.b64encode(file_bytes).decode('utf-8')
                print(f"Subiendo {filename} ({len(file_bytes)} bytes)...")
                send_transaction(sock, 'PSTPR', f'upload;{file_data_base64};{user_id};{filename}')
            except Exception as e:
                print(f"[ERROR] No se pudo leer o codificar el archivo: {e}")

        elif choice == '5':
            user_id = input("  ID Usuario que genera el log: ")
            accion = input("  Acción (ej: 'login_fallido'): ")
            desc = input("  Descripción del evento: ")
            send_transaction(sock, 'LOGAU', f'addlog;{user_id};{accion};{desc};INFO')

        elif choice == '6':
            user_id = input("  ID de usuario a notificar: ")
            mensaje = input("  Mensaje de la notificación: ")
            tipo = input("  Tipo de notificación (ej: 'Alerta', 'Info'): ")
            send_transaction(sock, 'NOTIF', f'sendalert;{user_id};{mensaje};{tipo}')

        elif choice == '7':
            user_id = input("  Ingrese su ID de usuario para ver sus notificaciones: ")
            send_transaction(sock, 'NOTIF', f'getnotif;{user_id}')

        elif choice == '0':
            break
        else:
            print("Opción no válida. Intente de nuevo.")

    print("Cerrando conexión.")
    sock.close()

if __name__ == "__main__":
    main_menu()

