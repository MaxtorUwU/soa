import socket, requests, os

ES_URL = os.getenv("ES_HOST") + "/emails/_search"

def handle_tx(tx):
    op, *params = tx.split(";")
    if op == "search":
        query = params[0]
        body = {
            "query": {"multi_match": {
                "query": query,
                "fields": ["remitente", "destinatario", "asunto", "contenido"]
            }}
        }
        res = requests.get(ES_URL, json=body).json()
        return str(res.get("hits", {}).get("hits", []))[:300]
    return "NK"

def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5200))
    s.listen()
    print("[CORRE] Activo en puerto 5200, esperando conexiones...")
    while True:
        conn, _ = s.accept()
        tx = conn.recv(4096).decode()
        print("[CORRE] TX:", tx)
        response = handle_tx(tx)
        conn.sendall(response.encode())
        conn.close()

if __name__ == "__main__":
    start_service()
