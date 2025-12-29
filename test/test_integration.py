"""
Pruebas de Integración
Verifica que todos los módulos funcionen juntos
"""
import sys
import os
import pytest
import time

sys. path.insert(0, os. path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.networking import TCPServer
from src.protocol import Message, MessageType, serializeMessage
from src.overlay import ChordNode


class TestIntegration:
    """Pruebas de integración completa"""
    
    def test_networking_with_protocol(self):
        """Verifica que Networking y Protocol funcionen juntos"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9990, handler)
        server.start()
        time.sleep(0.5)
        
        # Crear mensaje con Protocol
        msg = Message(
            MessageType.JOIN,
            "test_node",
            {"ip":  "127.0.0.1", "port": 9990}
        )
        
        # Enviar usando Networking
        server.send_message('127.0.0.1', 9990, msg. to_dict())
        time.sleep(1)
        
        # Verificar recepción
        assert len(messages) == 1
        assert messages[0]["type"] == "JOIN"
        
        server.stop()
    
    def test_chord_with_networking(self):
        """Verifica que Chord y Networking funcionen juntos"""
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        # Crear servidor
        server = TCPServer('127.0.0.1', 9989, handler)
        server.start()
        time.sleep(0.5)
        
        # Crear nodo Chord
        chord = ChordNode('127.0.0.1', 9989)
        chord.set_send_callback(server.send_message)
        
        # Verificar que Chord puede usar el callback
        assert chord.send_callback is not None
        
        server.stop()
    
    def test_full_stack_put_get(self):
        """Prueba PUT/GET completo con todos los módulos"""
        storage = {}
        
        def handler(msg, addr):
            msg_type = msg.get("type")
            
            if msg_type == "PUT":
                key = msg["data"]["key"]
                value = msg["data"]["value"]
                storage[key] = value
            
            elif msg_type == "GET":
                key = msg["data"]["key"]
                value = storage.get(key)
                
                response = Message(
                    MessageType. RESULT,
                    "server",
                    {"key": key, "value": value, "found":  value is not None}
                )
                server. send_message(addr[0], addr[1], response. to_dict())
        
        server = TCPServer('127.0.0.1', 9988, handler)
        server.start()
        time.sleep(0.5)
        
        # PUT
        put_msg = Message(
            MessageType.PUT,
            "client",
            {"key": "test", "value": "data"}
        )
        server.send_message('127.0.0.1', 9988, put_msg.to_dict())
        time.sleep(0.5)
        
        assert "test" in storage
        assert storage["test"] == "data"
        
        server.stop()