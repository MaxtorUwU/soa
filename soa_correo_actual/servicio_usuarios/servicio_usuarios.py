import socket
import psycopg2
import os
import bcrypt # <-- NUEVO: Importamos la librería de cifrado

# Conexión a la base de datos (sin cambios)
conn_pg = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST")
)

def handle_tx(tx):
    op, *params = tx.split(";")
    cur = conn_pg.cursor()
    
    # --- LÓGICA DE LOGIN ACTUALIZADA ---
    if op == "login":
        email, plain_text_pw = params
        
        # 1. Buscamos al usuario solo por email para obtener su hash
        cur.execute("SELECT contrasena FROM usuarios WHERE email=%s", (email,))
        result = cur.fetchone()
        
        if result:
            # 2. Si el usuario existe, extraemos el hash guardado de la BD
            hashed_pw_from_db = result[0].encode('utf-8')
            
            # 3. Comparamos la contraseña en texto plano con el hash
            #    bcrypt.checkpw se encarga de todo de forma segura
            if bcrypt.checkpw(plain_text_pw.encode('utf-8'), hashed_pw_from_db):
                return "OK: Login exitoso" # Contraseña correcta
        
        return "NK: Email o contraseña incorrectos" # Si el usuario no existe o la contraseña es incorrecta

    # --- LÓGICA DE REGISTRO ACTUALIZADA ---
    elif op == "regis":
        nombre, email, plain_text_pw, rol = params
        
        # 1. Convertimos la contraseña de texto plano a bytes
        password_bytes = plain_text_pw.encode('utf-8')
        
        # 2. Creamos un "salt" y generamos el hash seguro
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(password_bytes, salt)
        
        # 3. Guardamos el HASH (convertido a string) en la base de datos, NO la contraseña original
        cur.execute("INSERT INTO usuarios (nombre,email,contrasena,rol) VALUES (%s,%s,%s,%s)",
                    (nombre, email, hashed_password.decode('utf-8'), rol == "True"))
        conn_pg.commit()
        return "OK: Usuario registrado"
        
    return "NK: Operacion desconocida"

# Función start_service (sin cambios)
def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5100))
    s.listen()
    print("[USUAR] Activo en puerto 5100, esperando conexiones...")
    while True:
        conn, _ = s.accept()
        tx = conn.recv(4096).decode()
        print("[USUAR] TX:", tx)
        response = handle_tx(tx)
        conn.sendall(response.encode())
        conn.close()

if __name__ == "__main__":
    start_service()