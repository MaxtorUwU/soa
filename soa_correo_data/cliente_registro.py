import socket
import sys

# --- Configuración del Cliente ---
ESB_HOST = 'localhost'
ESB_PORT = 5000  # Puerto del ESB que definimos

# --- Datos del Nuevo Usuario ---
# ¡Modifica estos valores!
NOMBRE = "MAXIMUS CAROZZIS"
EMAIL = "maximus@carozzis.com"
PASSWORD = "GIADACH_SOA123"
ES_ADMIN = "True"  # "True" o "False"
# -----------------------------

# 1. Definir el servicio y los datos (según el PDF)
servicio = "USUAR"
datos = f"regis;{NOMBRE};{EMAIL};{PASSWORD};{ES_ADMIN}"

# 2. Armar el payload (SSSSS + datos)
payload = servicio + datos

# 3. Calcular el largo (NNNNN)
largo_payload = len(payload)
tx_completa = f"{largo_payload:05d}{payload}"

# 4. Enviar la transacción
try:
    print(f"[CLIENTE] Conectando a ESB en {ESB_HOST}:{ESB_PORT}...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ESB_HOST, ESB_PORT))

    print(f"[CLIENTE] Enviando TX: {tx_completa}")
    s.sendall(tx_completa.encode())

    # 5. Esperar la respuesta
    s.settimeout(5.0) # Espera 5 segundos
    respuesta = s.recv(1024).decode()
    
    # El bus también responde con NNNNN, la quitamos para ver el mensaje
    respuesta_limpia = respuesta[5:] 
    
    print(f"[CLIENTE] Respuesta del ESB: {respuesta_limpia}")

    s.close()

except socket.timeout:
    print("[CLIENTE] ERROR: No se recibió respuesta del ESB (Timeout).")
except ConnectionRefusedError:
    print(f"[CLIENTE] ERROR: No se pudo conectar. ¿Está corriendo 'docker-compose up'?")
except Exception as e:
    print(f"[CLIENTE] ERROR: {e}")