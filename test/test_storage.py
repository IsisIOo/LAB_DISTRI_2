"""
Pruebas unitarias Módulo 4: Almacenamiento Distribuido
"""

import pytest
import time
from unittest.mock import Mock
from src.storage import DistributedStorage

@pytest.fixture
def storage():
    send_cb = Mock()
    return DistributedStorage("a1b2c3d4e5f67890", send_cb)

def test_hash_key(storage):
    """Verifica cálculo de hash consistente"""
    assert len(storage.hash_key("test")) == 40  # SHA-1 hex
    assert storage.hash_key("test") == storage.hash_key("test")

def test_store_get_local(storage):
    """PUT/GET local básico"""
    storage.store_local("clave1", "valor1")
    result = storage.get_local("clave1")
    assert result["value"] == "valor1"
    assert result["is_replica"] == False

def test_replication_metadata(storage):
    """Metadata de réplicas"""
    storage.store_local("rep1", "valor_rep", is_replica=True)
    result = storage.get_local("rep1")
    assert result["is_replica"] == True

def test_handle_put(storage):
    """Procesamiento mensaje PUT"""
    msg = {
        "type": "PUT",
        "data": {"key": "test_put", "value": "123"}
    }
    response = storage.handle_storage_message(msg)
    assert response["type"] == "RESULT"
    assert response["data"]["status"] == "stored"

def test_handle_get(storage):
    """Procesamiento mensaje GET"""
    storage.store_local("test_get", "found")
    msg = {"type": "GET", "data": {"key": "test_get"}}
    response = storage.handle_storage_message(msg)
    assert response["data"]["found"] == True
    assert response["data"]["value"] == "found"

def test_handle_get_missing(storage):
    """GET clave inexistente"""
    msg = {"type": "GET", "data": {"key": "missing"}}
    response = storage.handle_storage_message(msg)
    assert response["data"]["found"] == False

def test_stats(storage):
    """Estadísticas de storage"""
    storage.store_local("p1", "v1")
    storage.store_local("r1", "v2", is_replica=True)
    stats = storage.get_stats()
    assert stats["total_keys"] == 2
    assert stats["primaries"] == 1

def test_is_responsible(storage):
    """Determinación de responsabilidad"""
    key_hash = storage.hash_key("mykey")
    assert storage.is_responsible(key_hash) == key_hash.startswith(storage.node_id[:8])
