"""
Pruebas para el Módulo 1: Networking
Verifica que el servidor TCP funcione correctamente
"""
import pytest
import time
import threading
import sys
import os

# Agregar src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.networking import TCPServer


class TestTCPServer: 
    """Pruebas para TCPServer"""
    
    def test_server_starts_and_stops(self):
        """Verifica que el servidor puede iniciar y detenerse"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9998, handler)
        server.start()
        time.sleep(0.5)
        
        assert server._running == True
        
        server.stop()
        time.sleep(0.5)
        assert server._running == False
    
    def test_send_and_receive_message(self):
        """Prueba envío y recepción de mensajes"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9997, handler)
        server.start()
        time.sleep(0.5)
        
        # Enviar mensaje
        test_msg = {
            "type": "TEST",
            "data": "Hello World"
        }
        
        server.send_message('127.0.0.1', 9997, test_msg)
        time.sleep(1)
        
        # Verificar recepción
        assert len(messages) == 1
        assert messages[0]["type"] == "TEST"
        assert messages[0]["data"] == "Hello World"
        
        server.stop()
    
    def test_multiple_messages(self):
        """Prueba envío de múltiples mensajes"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9996, handler)
        server.start()
        time.sleep(0.5)
        
        # Enviar 5 mensajes
        for i in range(5):
            msg = {"type": "TEST", "number": i}
            server.send_message('127.0.0.1', 9996, msg)
            time.sleep(0.1)
        
        time.sleep(1)
        
        assert len(messages) == 5
        assert messages[0]["number"] == 0
        assert messages[4]["number"] == 4
        
        server.stop()
    
    def test_concurrent_connections(self):
        """Prueba múltiples conexiones simultáneas"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9995, handler)
        server.start()
        time.sleep(0.5)
        
        # Enviar mensajes desde múltiples threads
        def send_messages(n):
            for i in range(3):
                msg = {"type":  "CONCURRENT", "thread":  n, "msg": i}
                server.send_message('127.0.0.1', 9995, msg)
                time.sleep(0.05)
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=send_messages, args=(i,))
            threads. append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        time.sleep(1)
        
        assert len(messages) == 9  # 3 threads * 3 mensajes
        
        server.stop()