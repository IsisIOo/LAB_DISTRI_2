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
        """Maneja PUT: almacena + replica"""
        data = msg.get("data", {})
        key = data.get("key")
        value = data.get("value")
        
        if not key or value is None:
            return self._error_response(request_id, "Key o value inválido")
        
        if self.store_local(key, value, is_replica=False):
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
        """Lookup Chord: forwarding hacia nodo responsable"""
        data = msg.get("data", {})
        key = data.get("key")
        key_hash = self.hash_key(key)
        hops = data.get("hops", 0)
        
        print(f"🔍 [{self.node_id[:8]}] Lookup {key} (hash={key_hash[:8]}, hops={hops+1})")
        
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
        
        hops += 1
        if hops > 10:
            return self._error_response(request_id, "Lookup timeout")
        
        print(f"↪️ Forward lookup a successor (hops={hops})")
        return None
    
    # Interfaz pública para PUT distribuido
    def put(self, key: str, value: Any) -> dict:
        """PUT distribuido: retorna mensaje listo para enviar"""
        from src.protocol import Message, MessageType
        
        request_id = f"PUT_{self.node_id[:8]}_{int(time.time())}"
        msg = Message(
            msg_type=MessageType.PUT,
            sender_id=self.node_id[:8],
            data={"key": key, "value": value}
        )
        print(f"📤 PUT distribuido: {key} (usa chord.get_responsible_node(key))")
        return {"request_id": request_id, "status": "sent", "message": msg.to_dict()}
    
    def get(self, key: str) -> dict:
        """GET distribuido: retorna mensaje listo para enviar"""
        from src.protocol import Message, MessageType
        
        request_id = f"GET_{self.node_id[:8]}_{int(time.time())}"
        msg = Message(
            msg_type=MessageType.GET,
            sender_id=self.node_id[:8],
            data={"key": key}
        )
        print(f"🔍 GET distribuido: {key} (usa chord.get_responsible_node(key))")
        return {"request_id": request_id, "status": "searching", "message": msg.to_dict()}
    
    def _replicate_to_successors(self, key: str, value: Any, request_id: str):
        """Replica en R-1 nodos sucesivos (usa main.py para enviar)"""
        print(f"🔄 Réplicas para {key} preparadas (R={self.replication_factor-1})")
        print(f"   → Usa chord.successor para enviar REPLICATE messages")
    
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
