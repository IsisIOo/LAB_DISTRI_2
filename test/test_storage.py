import pytest
from unittest.mock import Mock
from src.storage import DistributedStorage

@pytest.fixture
def storage():
    send_cb = Mock()
    return DistributedStorage("a1b2c3d4e5f67890", send_cb)

def test_hash_key(storage):
    assert len(storage.hash_key("test")) == 40
    assert storage.hash_key("test") == storage.hash_key("test")

def test_store_get_local(storage):
    storage.store_local("clave1", "valor1")
    result = storage.get_local("clave1")
    assert result["value"] == "valor1"
    assert result["is_replica"] == False

def test_replication_metadata(storage):
    storage.store_local("rep1", "valor_rep", is_replica=True)
    result = storage.get_local("rep1")
    assert result["is_replica"] == True

def test_handle_put(storage):
    msg = {"type": "PUT", "data": {"key": "test_put", "value": "123"}}
    response = storage.handle_storage_message(msg)
    assert response["type"] == "RESULT"
    assert response["data"]["status"] == "stored"

def test_handle_get(storage):
    storage.store_local("test_get", "found")
    msg = {"type": "GET", "data": {"key": "test_get"}}
    response = storage.handle_storage_message(msg)
    assert response["data"]["found"] == True
    assert response["data"]["value"] == "found"

def test_handle_get_missing(storage):
    msg = {"type": "GET", "data": {"key": "missing"}}
    response = storage.handle_storage_message(msg)
    assert response["data"]["found"] == False

def test_stats(storage):
    storage.store_local("p1", "v1")
    storage.store_local("r1", "v2", is_replica=True)
    stats = storage.get_stats()
    assert stats["total_keys"] == 2
    assert stats["primaries"] == 1

def test_is_responsible(storage):
    key_hash = storage.hash_key("mykey")
    assert storage.is_responsible(key_hash) == key_hash.startswith(storage.node_id[:8])

def test_put(storage):
    result = storage.put("test_key", "test_value")
    assert result["status"] == "sent"
    assert "message" in result
    assert result["message"]["type"] == "PUT"

def test_get(storage):
    result = storage.get("test_key")
    assert result["status"] == "searching"
    assert "message" in result
    assert result["message"]["type"] == "GET"
