import time
from src.networking import TCPServer

def handle_incoming_message(msg: dict, addr: tuple):
    """Procesa mensajes recibidos"""
    print("\n" + "="*60)
    print(f"MENSAJE RECIBIDO desde {addr[0]}:{addr[1]}")
    print("="*60)
    print(f"Tipo: {msg. get('type')}")
    print(f"Remitente ID: {msg.get('sender_id')}")
    print(f"Datos: {msg.get('data')}")
    print(f"Timestamp: {msg.get('timestamp')}")
    print("="*60 + "\n")

def main():
    print("="*60)
    print("SERVIDOR - Esperando mensajes")
    print("="*60)
    
    # IMPORTANTE: Usa un puerto > 1024 (ej: 5000)
    puerto = 5000
    
    # 0.0.0.0 escucha en todas las interfaces de red
    servidor = TCPServer('0.0.0.0', puerto, handle_incoming_message)
    servidor.start()
    
    print(f"\nServidor escuchando en puerto {puerto}")
    print(f" Esperando conexiones...")
    print(f"\nComparte esta información con el cliente:")
    print(f"   - IP del servidor: [Averigua tu IP local]")
    print(f"   - Puerto:  {puerto}")
    print(f"\nPara detener el servidor:  Ctrl+C\n")
    
    # Mantener servidor activo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nDeteniendo servidor...")
        servidor.stop()
        print("Servidor cerrado")

if __name__ == "__main__":
    main()