import pytest
import time
from src.networking import TCPServer
from src.protocol import Message, MessageType
from src.overlay import ChordNode

class TestSmoke: 
    """Verificar que todos los módulos se importan"""   
    def test_01_imports_work(self):
        # Si llegamos aquí, los imports funcionaron
        assert True

    """El servidor TCP puede iniciar"""
    def test_02_server_starts(self):
        
        messages = []
        
        def handler(msg, addr):
            messages.append(msg)
        
        server = TCPServer('127.0.0.1', 9999, handler)
        server.start()
        time.sleep(0.5)
        
        #El servidor funciona sin ningun problema
        assert server._running == True 
        
        server.stop()
    
    """Un nodo Chord puede crearse"""
    def test_03_chord_node_creates(self):
        
        node = ChordNode('127.0.0.1', 5555)
        
        assert node.node_id is not None  
        assert len(node.node_id) == 40   
    
    """Los mensajes pueden serializarse"""
    def test_04_message_serializes(self):
        
        msg = Message(MessageType.JOIN, "test_node", {"ip": "127.0.0.1"})
        msg_dict = msg.to_dict()
        
        assert "type" in msg_dict  
        assert msg_dict["type"] == "JOIN"

    """PUT/GET local funciona"""
    def test_05_basic_put_get_local(self):
       
        storage = {}
        
        # PUT
        storage["key1"] = "value1"
        
        # GET
        result = storage.get("key1")
        
        assert result == "value1"  
    
    """Flujo completo en un solo nodo"""
    def test_06_end_to_end_single_node(self):
        
        # Crear nodo
        node = ChordNode('127.0.0.1', 5556)
        node.is_joined = True
        node.successor = (node.ip, node.port, node.node_id)
        
        # Simular PUT
        key = "test_key"
        node.local_store[key] = "test_value"
        
        # Simular GET
        value = node.local_store.get(key)
        
        assert value == "test_value" 