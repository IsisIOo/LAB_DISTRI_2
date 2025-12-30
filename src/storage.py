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
    def __init__(self, node_id: str, send_callback, chord=None):
        self.node_id = node_id
        self.send_callback = send_callback
        self.chord = chord  # Para routing
        self.local_storage: Dict[str, dict] = {}
        self.pending_requests: Dict[str, dict] = {}  # {req_id: {"future": future}}
        self.replication_factor = 2
        self.request_timeout = 5.0
        
        # Hilo para timeouts
        self.timeout_thread = threading.Thread(target=self._timeout_checker, daemon=True)
        self.timeout_thread.start()

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
        """PUT distribuido asíncrono"""
        from src.protocol import Message, MessageType
        
        request_id = f"PUT_{self.node_id[:8]}_{int(time.time())}"
        msg = Message(MessageType.PUT, self.node_id[:8], {"key": key, "value": value})
        
        if self.chord:
            responsible = self.chord.get_responsible_node(key)
            if responsible:
                self.send_callback(responsible[0], responsible[1], msg.to_dict())
                print(f"📤 PUT {key} → {responsible[2][:8]}")
        
        return {"request_id": request_id, "status": "sent"}
    
    def get(self, key: str, timeout: float = None) -> Optional[dict]:
        timeout = timeout or self.request_timeout
        
        # ⭐ OBTENER IP/PUERTO ACTUAL (desde main.py globals o chord)
        sender_ip = getattr(self.chord, 'mi_ip', '192.168.0.14')  # ← FIX 1
        sender_port = getattr(self.chord, 'mi_puerto', 15000)      # ← FIX 2
        
        request_id = f"GET_{self.node_id[:8]}_{int(time.time())}"
        msg = {
            "type": "GET",
            "request_id": request_id,
            "sender_id": self.node_id[:8],
            "sender_ip": sender_ip,      # ← FIX 3 ⭐ CRÍTICO
            "sender_port": sender_port,  # ← FIX 4 ⭐ CRÍTICO
            "data": {"key": key}
        }
        
        # Crear future
        future = {"result": None, "error": None, "done": threading.Event()}
        self.pending_requests[request_id] = future
        
        # Enviar si hay chord
        if self.chord:
            responsible = self.chord.get_responsible_node(key)
            if responsible:
                print(f"🔍 GET {key} → {responsible[2][:8]} ({responsible[0]}:{responsible[1]})")
                self.send_callback(responsible[0], responsible[1], msg)  # ← SIN .to_dict()
            else:
                future["error"] = "no_responsible"
                future["done"].set()
    
    def _handle_result(self, msg: dict):
        """Procesa respuestas RESULT entrantes"""
        request_id = msg.get("request_id")
        if request_id and request_id in self.pending_requests:
            future = self.pending_requests[request_id]
            future["result"] = msg.get("data")
            future["done"].set()
            del self.pending_requests[request_id]
    
    def _timeout_checker(self):
        """Limpia requests expirados"""
        while True:
            time.sleep(1)
            now = time.time()
            expired = [rid for rid, req in self.pending_requests.items() 
                      if now - req.get("sent_time", 0) > self.request_timeout]
            for rid in expired:
                self.pending_requests[rid]["error"] = "timeout"
                self.pending_requests[rid]["done"].set()
                del self.pending_requests[rid]
    
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
