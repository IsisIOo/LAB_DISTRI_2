import time
from src.networking import TCPServer

def handle_incoming_message(msg: dict, addr: tuple):
    """
    Callback que se ejecuta cuando llega un mensaje al servidor.
    Aquí solo se imprime; en tu proyecto real, deberías:
    - Validar tipo de mensaje (JOIN, PUT, GET, HEARTBEAT, etc.).
    - Pasarlo al módulo de protocolo / overlay / storage.
    """
    print(f"[handler] Mensaje recibido desde {addr}: {msg}")

def main():
  pepe = TCPServer('0.0.0.0', 96, handle_incoming_message)
  pepe.start()

  print("Esperando 5 segundos para que se inicie el servidor")
  for i in range(5):
    time.sleep(1)  # Pequeña espera para asegurar que el server ya escucha
    print(f"Faltan {5 - i} segundos...")

  test_msg = {
      "type": "JOIN",
      "sender_id": 1,
      "data": {
          "ip": "127.0.0.1",
          "port": 96,
      },
  }
  pepe.send_message("127.0.0.1", 96, test_msg)
  # Mantener el proceso vivo para seguir aceptando conexiones
  try:
      while True:
          time.sleep(1)
  except KeyboardInterrupt:
      print("Cerrando nodo...")
      pepe.stop()


if __name__ == "__main__":
  print("Ejecutando main")
  main()
