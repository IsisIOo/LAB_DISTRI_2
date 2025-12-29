# src/networking.py
import socket
import threading
import json
import logging
from typing import Callable, Dict, Any, Optional

# Configuración básica de logging con timestamp
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Tipo de callback: función que recibe el dict del mensaje y la dirección del cliente
# y devuelve opcionalmente una respuesta (dict) que será enviada por el mismo socket.
MessageHandler = Callable[[Dict[str, Any], tuple], Optional[Dict[str, Any]]]


class TCPServer:
    """
    Servidor TCP multi-thread simple para el laboratorio P2P.
    - Escucha conexiones entrantes.
    - Recibe mensajes en formato JSON (una línea por mensaje).
    - Llama a un handler para procesar cada mensaje.
    """

    def __init__(self, host: str, port: int, message_handler: MessageHandler):
        self.host = host
        self.port = port
        self.message_handler = message_handler
        self._server_socket = None
        self._running = False

    def start(self):
        """Inicia el servidor en un hilo separado."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Reusar dirección para reiniciar rápido el servidor
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen()

        self._running = True
        logging.info(f"Servidor TCP escuchando en {self.host}:{self.port}")

        thread = threading.Thread(target=self._accept_loop, daemon=True)
        thread.start()

    def stop(self):
        """Detiene el servidor."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        logging.info("Servidor TCP detenido")

    def _accept_loop(self):
        """Bucle principal para aceptar conexiones."""
        while self._running:
            try:
                client_socket, client_addr = self._server_socket.accept()
            except OSError:
                # El socket fue cerrado
                break

            logging.info(f"Nueva conexión desde {client_addr}")
            client_thread = threading.Thread(
                target=self._handle_client,
                args=(client_socket, client_addr),
                daemon=True,
            )
            client_thread.start()

    def _handle_client(self, client_socket: socket.socket, client_addr: tuple):
        """Maneja una conexión con un cliente (en un hilo separado)."""
        with client_socket:
            try:
                # Se asume que cada mensaje es una línea JSON terminada en '\n'
                buffer = ""
                while True:
                    data = client_socket.recv(4096)
                    if not data:
                        logging.info(f"Conexión cerrada por {client_addr}")
                        break
                    buffer += data.decode("utf-8")

                    # Procesar líneas completas
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                            logging.info(f"Mensaje recibido de {client_addr}: {msg}")
                            # Llamar al handler para que otro módulo procese el mensaje
                            response = self.message_handler(msg, client_addr)
                            # Si el handler retorna una respuesta, enviarla por el mismo socket
                            if response is not None:
                                try:
                                    json_str = json.dumps(response)
                                    client_socket.sendall((json_str + "\n").encode("utf-8"))
                                    logging.info(f"Respuesta enviada a {client_addr}: {response}")
                                except Exception as e:
                                    logging.error(f"Error enviando respuesta a {client_addr}: {e}")
                        except json.JSONDecodeError as e:
                            logging.error(
                                f"Error al parsear JSON desde {client_addr}: {e} | data={line}"
                            )
            except ConnectionResetError:
                logging.warning(f"Conexión reseteada por el cliente {client_addr}")
            except Exception as e:
                logging.exception(f"Error manejando conexión con {client_addr}: {e}")


    def send_message(self, ip: str, port: int, msg: Dict[str, Any], timeout: float = 5.0) -> bool:
        """
        Envía un mensaje JSON a (ip, port) usando TCP.
        - msg debe ser un dict serializable a JSON.
        - Se envía como una línea de texto terminada en '\n'.
        """
        json_str = json.dumps(msg)
        data = (json_str + "\n").encode("utf-8")
        logging.info(f"Enviando mensaje a {ip}:{port}: {msg}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
            sock.sendall(data)
            return True
        except (ConnectionRefusedError, socket.timeout) as e:
            logging.error(f"No se pudo enviar mensaje a {ip}:{port}: {e}")
            return False
        finally:
            sock.close()

    def request_response(self, ip: str, port: int, msg: Dict[str, Any], timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """
        Envía un mensaje JSON a (ip, port) y espera una respuesta JSON en la
        misma conexión. Retorna el dict de la respuesta o None si falla.
        """
        json_str = json.dumps(msg)
        data = (json_str + "\n").encode("utf-8")
        logging.info(f"Solicitando (request/response) a {ip}:{port}: {msg}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
            sock.sendall(data)

            buffer = ""
            # Esperar una sola línea de respuesta
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buffer += chunk.decode("utf-8")
                if "\n" in buffer:
                    line, _rest = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        return None
                    try:
                        response = json.loads(line)
                        logging.info(f"Respuesta recibida desde {ip}:{port}: {response}")
                        return response
                    except json.JSONDecodeError as e:
                        logging.error(f"Error parseando respuesta desde {ip}:{port}: {e} | data={line}")
                        return None
        except (ConnectionRefusedError, socket.timeout) as e:
            logging.error(f"Fallo request/response con {ip}:{port}: {e}")
        except Exception as e:
            logging.exception(f"Error en request/response con {ip}:{port}: {e}")
        finally:
            try:
                sock.close()
            except Exception:
                pass
        return None
