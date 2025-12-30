"""
Pruebas para el Módulo 2: Protocol
Verifica serialización, deserialización y validación de mensajes
"""
import sys
import os
import pytest
import time

sys.path.insert(0, os.path.abspath(os. path.join(os.path. dirname(__file__), '..')))

from src.protocol import Message, MessageType, serializeMessage, deserialize_message


class TestMessage: 
    """Pruebas para la clase Message"""
    
    def test_message_creation(self):
        """Verifica creación básica de mensaje"""
        msg = Message(
            msg_type=MessageType.JOIN,
            sender_id="test_node",
            data={"ip": "192.168.1.1", "port": 5000}
        )
        
        assert msg.type == MessageType.JOIN
        assert msg.sender_id == "test_node"
        assert msg. data["ip"] == "192.168.1.1"
        assert msg.timestamp is not None
    
    def test_message_to_dict(self):
        """Verifica conversión a diccionario"""
        msg = Message(
            msg_type=MessageType.PUT,
            sender_id="node1",
            data={"key":  "test", "value": "data"}
        )
        
        msg_dict = msg.to_dict()
        
        assert msg_dict["type"] == "PUT"
        assert msg_dict["sender_id"] == "node1"
        assert "timestamp" in msg_dict
        assert msg_dict["data"]["key"] == "test"
    
    def test_all_message_types(self):
        """Verifica que todos los tipos de mensajes funcionen"""
        for msg_type in MessageType: 
            msg = Message(msg_type, "test_sender", {"test": "data"})
            assert msg.type == msg_type


class TestSerialization:
    """Pruebas de serialización"""
    
    def test_serialize_and_deserialize(self):
        """Prueba ciclo completo de serialización"""
        original = Message(
            msg_type=MessageType.JOIN,
            sender_id="node_123",
            data={"ip":  "192.168.0.10", "port": 8080}
        )
        
        # Serializar
        json_str = serializeMessage(original)
        assert isinstance(json_str, str)
        
        # Deserializar
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType.JOIN
        assert reconstructed.sender_id == "node_123"
        assert reconstructed. data["ip"] == "192.168.0.10"
        assert reconstructed.data["port"] == 8080
    
    def test_preserve_timestamp(self):
        """Verifica que el timestamp se preserve"""
        msg = Message(MessageType. HEARTBEAT, "node1")
        original_time = msg.timestamp
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.timestamp == original_time
    
    def test_empty_data(self):
        """Verifica mensajes sin data"""
        msg = Message(MessageType.HEARTBEAT, "node1")
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.data == {}
    
    def test_invalid_json(self):
        """Verifica rechazo de JSON inválido"""
        with pytest.raises(ValueError):
            deserialize_message("{ esto no es json")
    
    def test_missing_fields(self):
        """Verifica rechazo de mensajes incompletos"""
        with pytest. raises(ValueError):
            deserialize_message('{"type": "JOIN"}')  # Falta sender_id
    
    def test_invalid_message_type(self):
        """Verifica rechazo de tipos inválidos"""
        with pytest. raises(ValueError):
            Message("INVALID_TYPE", "node1")

    def test_complex_data_structure(self):
        """Prueba serialización de datos complejos"""
        msg = Message(
            MessageType.PUT,
            "node1",
            {
                "key":  "user: 123",
                "value": {
                    "nombre": "Alice",
                    "edad": 30,
                    "activo": True,
                    "tags": ["admin", "premium"]
                }
            }
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.data["value"]["nombre"] == "Alice"
        assert reconstructed.data["value"]["edad"] == 30
        assert "admin" in reconstructed.data["value"]["tags"]
    
    def test_large_message(self):
        """Prueba serialización de mensaje grande"""
        large_data = {"key": "big_file", "value": "A" * 10000}  # 10KB
        
        msg = Message(MessageType.PUT, "node1", large_data)
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert len(reconstructed.data["value"]) == 10000


class TestMessageTypes:
    """Pruebas específicas por tipo de mensaje"""
    
    def test_join_message(self):
        """Prueba mensaje JOIN"""
        msg = Message(
            MessageType.JOIN,
            "new_node",
            {"ip":  "192.168.1.100", "port": 5000}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType.JOIN
        assert "ip" in reconstructed.data
    
    def test_put_message(self):
        """Prueba mensaje PUT"""
        msg = Message(
            MessageType.PUT,
            "storage_node",
            {"key":  "usuario:123", "value": "Alice"}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed. type == MessageType.PUT
        assert reconstructed.data["key"] == "usuario:123"
    
    def test_get_message(self):
        """Prueba mensaje GET"""
        msg = Message(
            MessageType.GET,
            "client_node",
            {"key": "usuario:123"}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType.GET
        assert "key" in reconstructed.data
    
    def test_result_message(self):
        """Prueba mensaje RESULT"""
        msg = Message(
            MessageType.RESULT,
            "server_node",
            {"key": "usuario:123", "value": "Alice", "found": True}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType.RESULT
        assert reconstructed.data["found"] == True
    
    def test_update_message(self):
        """Prueba mensaje UPDATE"""
        msg = Message(
            MessageType.UPDATE,
            "node1",
            {"mensaje": "Estado actualizado", "peers": 3}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType.UPDATE
        assert "mensaje" in reconstructed.data

    def test_heartbeat_message(self):
        """Prueba mensaje HEARTBEAT completo"""
        msg = Message(
            MessageType.HEARTBEAT,
            "node_alive",
            {"status": "alive"}
        )
        
        json_str = serializeMessage(msg)
        reconstructed = deserialize_message(json_str)
        
        assert reconstructed.type == MessageType. HEARTBEAT
        assert reconstructed.data["status"] == "alive"