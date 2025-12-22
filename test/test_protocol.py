# tests/test_protocol.py
import sys
import os
import pytest

# Ajuste para importar desde src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.protocol import Message, MessageType, serializeMessage, deserialize_message

def test_serialization_flow():
    """Prueba que un mensaje se serializa y deserializa sin perder datos"""
    # 1. Crear mensaje original
    original_msg = Message(
        msg_type=MessageType.JOIN,
        sender_id="nodo_thomas_123",
        data={"ip": "192.168.0.10", "port": 8080}
    )
    
    # 2. Serializar (Simular envío)
    json_str = serializeMessage(original_msg)
    print(f"\nMensaje serializado: {json_str}") # Para que veas como luce
    
    # 3. Deserializar (Simular recepción)
    new_msg = deserialize_message(json_str)
    
    # 4. Verificar que son iguales
    assert new_msg.type == MessageType.JOIN
    assert new_msg.sender_id == "nodo_thomas_123"
    assert new_msg.data["ip"] == "192.168.0.10"
    assert new_msg.data["port"] == 8080

def test_invalid_json():
    """Prueba que el sistema rechaza basura"""
    basura = "{ 'esto no es json valido"
    with pytest.raises(ValueError):
        deserialize_message(basura)