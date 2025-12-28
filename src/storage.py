"""
Módulo 4: Almacenamiento Distribuido y Búsqueda (DHT Chord)
- Tabla clave-valor local con replicación R=2
- Lookup distribuido estilo Chord
- PUT/GET distribuidos con confirmación
"""

import hashlib
import time
import threading
from typing import Dict, Optional, Tuple, Any
from src.protocol import Message, MessageType

class DistributedStorage:
    def __init__(self, node_id: str, send_callback):
        self.node_id = node_id
        self.send_callback = send_callback
        self.local_storage: Dict[str, dict] = {}  # {key: {"value": v, "timestamp": t, "replicas": 0}}
        self.pending_requests: Dict[str, dict] = {}  # {request_id: {"callback": cb, "timeout": t}}
        self.replication_factor = 2
        self.lookup_lock = threading.Lock()


    # Utiliza algoritmo SHA-1 para hashing de claves 
    def hash_key(self, key: str) -> str:
        return hashlib.sha1(key.encode()).hexdigest()
    
    # Ve si es responsable del hash dado (simplificado)
    def is_responsible(self, key_hash: str) -> bool:
        return key_hash.startswith(self.node_id[:8])  # Primeros 8 chars del hash
    
    # Almacena localmente con metadata
    def store_local(self, key: str, value: Any, is_replica: bool = False) -> bool:
        key_hash = self.hash_key(key)
        timestamp = time.time()
        
        self.local_storage[key] = {
            "value": value,
            "timestamp": timestamp,
            "key_hash": key_hash,
            "is_replica": is_replica,
            "replicas": 1 if not is_replica else 0
        }
        print(f"💾 [{self.node_id[:8]}] Almacenado: {key} (hash={key_hash[:8]}) {'[REPLICA]' if is_replica else '[PRIMARY]'}")
        return True
    
    # Dado una clave, obtiene el valor localmente
    def get_local(self, key: str) -> Optional[dict]:
        return self.local_storage.get(key)
    
    # Procesa mensajes de almacenamiento entrantes
    def handle_storage_message(self, msg: dict) -> Optional[dict]:
        msg_type = msg.get("type", "")
        request_id = msg.get("request_id", f"{msg.get('sender_id', '')}_{int(time.time())}")
        
        if msg_type == "PUT":
            return self._handle_put(msg, request_id)
        elif msg_type == "GET":
            return self._handle_get(msg, request_id)
        elif msg_type == "REPLICATE":
            return self._handle_replicate(msg, request_id)
        elif msg_type == "LOOKUP":
            return self._handle_lookup(msg, request_id)
        
        return None
    
    # Maneja PUT: almacena + replica en R-1 nodos sucesivos
    def _handle_put(self, msg: dict, request_id: str) -> Optional[dict]:
        data = msg.get("data", {})
        key = data.get("key")
        value = data.get("value")
        
        if not key or value is None:
            return self._error_response(request_id, "Key o value inválido")
        
        # 1. Almacenar localmente como primario
        if self.store_local(key, value, is_replica=False):
            # 2. Replicar en los siguientes R-1 nodos (simplificado: successor)
            self._replicate_to_successors(key, value, request_id)
            
            return {
                "type": "RESULT",
                "request_id": request_id,
                "sender_id": self.node_id[:8],
                "data": {
                    "status": "stored",
                    "key": key,
                    "node": self.node_id[:8],
                    "replicas": self.replication_factor
                }
            }
        return self._error_response(request_id, "Error al almacenar")
    
    # Maneja GET: devuelve valor si existe localmente
    def _handle_get(self, msg: dict, request_id: str) -> Optional[dict]:
        data = msg.get("data", {})
        key = data.get("key")
        
        result = self.get_local(key)
        if result:
            return {
                "type": "RESULT",
                "request_id": request_id,
                "sender_id": self.node_id[:8],
                "data": {
                    "key": key,
                    "value": result["value"],
                    "found": True,
                    "node": self.node_id[:8],
                    "timestamp": result["timestamp"]
                }
            }
        return {
            "type": "RESULT",
            "request_id": request_id,
            "sender_id": self.node_id[:8],
            "data": {"key": key, "found": False, "node": self.node_id[:8]}
        }
    
    # Maneja REPLICATE: almacena como réplica
    def _handle_replicate(self, msg: dict, request_id: str) -> Optional[dict]:
        data = msg.get("data", {})
        key = data.get("key")
        value = data.get("value")
        
        if self.store_local(key, value, is_replica=True):
            return {
                "type": "ACK",
                "request_id": request_id,
                "sender_id": self.node_id[:8],
                "data": {"status": "replicated", "key": key}
            }
        return self._error_response(request_id, "Error en replicación")
    
    # Maneja LOOKUP: busca clave en DHT estilo Chord
    def _handle_lookup(self, msg: dict, request_id: str) -> Optional[dict]:
        data = msg.get("data", {})
        key = data.get("key")
        key_hash = self.hash_key(key)
        origin = msg.get("origin", {})
        hops = data.get("hops", 0)
        
        print(f"🔍 [{self.node_id[:8]}] Lookup {key} (hash={key_hash[:8]}, hops={hops+1})")
        
        # Si soy responsable o tengo la clave
        if key in self.local_storage or self.is_responsible(key_hash):
            result = self.get_local(key)
            return {
                "type": "RESULT",
                "request_id": request_id,
                "sender_id": self.node_id[:8],
                "data": {
                    "key": key,
                    "value": result["value"] if result else None,
                    "found": result is not None,
                    "node": self.node_id[:8],
                    "final": True
                }
            }
        
        # Forward al successor (simplificado)
        hops += 1
        if hops > 10:  # Max hops
            return self._error_response(request_id, "Lookup timeout")
        
        # Reenviar al successor (aquí necesitarías integración con Chord)
        forward_msg = {
            "type": "LOOKUP",
            "sender_id": self.node_id[:8],
            "origin": origin,
            "request_id": request_id,
            "data": {"key": key, "hops": hops}
        }
        # TODO: Enviar a chord.successor[0:2]
        print(f"↪️ Forward lookup a successor (hops={hops})")
        
        return None  # No responder, continua lookup
    
    def put(self, key: str, value: Any) -> dict:
        """PUT distribuido: envía a nodo responsable"""
        request_id = f"PUT_{self.node_id[:8]}_{int(time.time())}"
        msg = Message(
            msg_type=MessageType.PUT,
            sender_id=self.node_id[:8],
            request_id=request_id,
            data={"key": key, "value": value}
        )
        # TODO: Enviar a chord.get_responsible_node(key)
        print(f"📤 PUT distribuido: {key}")
        return {"request_id": request_id, "status": "sent"}
    
    def get(self, key: str) -> dict:
        """GET distribuido: lookup estilo Chord"""
        request_id = f"GET_{self.node_id[:8]}_{int(time.time())}"
        msg = Message(
            msg_type=MessageType.GET,
            sender_id=self.node_id[:8],
            request_id=request_id,
            data={"key": key}
        )
        # TODO: Iniciar lookup Chord
        print(f"🔍 GET distribuido: {key}")
        return {"request_id": request_id, "status": "searching"}
    
    def _replicate_to_successors(self, key: str, value: Any, request_id: str):
        """Replica en R-1 nodos sucesivos"""
        for i in range(1, self.replication_factor):
            rep_msg = {
                "type": "REPLICATE",
                "request_id": f"REP_{request_id}_{i}",
                "sender_id": self.node_id[:8],
                "data": {"key": key, "value": value}
            }
            # TODO: Enviar a i-ésimo successor
            print(f"🔄 Replicando {key} a réplica #{i}")
    
    # Respuesta de error genérica
    def _error_response(self, request_id: str, error: str) -> dict:
        return {
            "type": "ERROR",
            "request_id": request_id,
            "sender_id": self.node_id[:8],
            "data": {"error": error}
        }
    
    def get_stats(self) -> dict:
        """Estadísticas del storage"""
        primaries = sum(1 for v in self.local_storage.values() if not v.get("is_replica", False))
        replicas = len(self.local_storage) - primaries
        return {
            "total_keys": len(self.local_storage),
            "primaries": primaries,
            "replicas": replicas,
            "replication_factor": self.replication_factor
        }
