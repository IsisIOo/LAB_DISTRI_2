#MODULO 3: OVERLAY - CORD
#Anillo hash
import hashlib
import json
import socket
import threading
import time
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
import logging

#Importar protocol.py para obtener los mensajes disponibles
try:
    from src.protocol import MessageType, Message, serialize_message, deserialize_message
    PROTOCOL_AVAILABLE = True
except ImportError:
    print("protocol.py no disponible.")
    PROTOCOL_AVAILABLE = False

# Configurar logging para debugging (se puede borrar luego)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Chord] %(message)s'
)
logger = logging.getLogger(__name__)

"""
Nodo del anillo Chord reducido.
chordnode: Mantiene successor, predecessor y tabla de finger simplificada.
"""
class ChordNode:

    """
    descripcion: Inicializa un nuevo nodo Chord
    entrada: ip Dirección IP del nodo, port Puerto del nodo, 
    existing_node (ip, port) de un nodo existente para unirse al anillo
    salida: -
    """
    # LÍNEA 39 - En __init__, AGREGAR este parámetro:
    def __init__(self, ip: str, port: int, existing_node:  Tuple[str, int] = None, send_callback = None):
        self.ip = ip
        self.port = port
        self.send_callback = send_callback  # ⭐ AGREGAR ESTA LÍNEA

        
        # 1. CALCULAR EL ID DEL NODO (Hash SHA-1 de ip:port)
        node_string = f"{ip}:{port}"
        self.node_id = self._calculate_hash(node_string)
        logger.info(f"Nodo creado: ID={self.node_id[:8]}... ({ip}:{port})")
        
        # 2. ESTRUCTURAS DEL ANILLO (ESENCIALES)
        self.successor: Optional[Tuple[str, int, str]] = None  # (ip, port, node_id)
        self.predecessor: Optional[Tuple[str, int, str]] = None  # (ip, port, node_id)
        
        # 3. TABLA DE FINGER SIMPLIFICADA (para Chord reducido)
        self.finger_table: List[Tuple[str, int, str]] = []  # Lista de (ip, port, node_id)
        
        # 4. ALMACENAMIENTO LOCAL (para pruebas)
        self.local_store: Dict[str, Any] = {}
        
        # 5. ESTADO Y CONFIGURACIÓN
        self.is_joined = False
        self.running = True
        
        # 6. HILOS PARA OPERACIONES PERIÓDICAS
        self.stabilize_thread = None
        self.fix_fingers_thread = None
        self.check_predecessor_thread = None
        
        # 7. UNIRSE AL ANILLO SI SE PROVEE NODO EXISTENTE
        if existing_node:
            self.join_network(existing_node)
    
    # ==================== FUNCIONES ESENCIALES DE HASH ====================
    """
    _calculate_hash
    descripcion: calcula el hash SHA-1 de una cadena
    entrada: key cadena que se debe hashear
    salida: string de hash hexadecimal de 40 caracteres
    """
    def _calculate_hash(self, key: str) -> str:
        return hashlib.sha1(key.encode()).hexdigest()
    
    """
    is_between
    descripcion: Verifica si una clave está en cierto intervalo en el anillo.
    entrada: key hash a verificar, start inicio del intervalo, end fin del intervalo, inclusive si end es inclusivo
    salida: booleano indicando si key está en el intervalo
    """  
    def _is_between(self, key: str, start: str, end: str, inclusive: bool = False) -> bool:
        # Convertir hashes a enteros para comparación
        key_int = int(key, 16)
        start_int = int(start, 16)
        end_int = int(end, 16)
        
        if start_int < end_int:
            if inclusive:
                return start_int < key_int <= end_int
            else:
                return start_int < key_int < end_int
        else:  # Intervalo que cruza el "0"
            if inclusive:
                return start_int < key_int or key_int <= end_int
            else:
                return start_int < key_int or key_int < end_int
            
    #Función para devolver el mensaje de que se envio sin problemas
    def set_send_callback(self, callback):
        """Configura la función para enviar mensajes"""
        self.send_callback = callback
    
    # ==================== OPERACIONES BÁSICAS DEL ANILLO ====================
    
    
    """join_network
    descripcion: Une este nodo al anillo Chord usando un nodo existente.
    entrada: existing_node (ip, port) de un nodo existente
    salida: -
    """
    def join_network(self, existing_node: Tuple[str, int]):
        existing_ip, existing_port = existing_node
        
        logger.info(f"Uniéndose al anillo a través de {existing_ip}:{existing_port}")
        
        try:
            # 1. Contactar al nodo existente para encontrar nuestro successor
            successor = self._find_successor_remote(self.node_id, existing_ip, existing_port)
            
            if successor:
                # 2. Asignar successor
                succ_ip, succ_port, succ_id = successor
                self.successor = (succ_ip, succ_port, succ_id)
                
                # 3. Notificar al successor que somos su nuevo predecessor
                self._notify_successor(succ_ip, succ_port)
                
                # 4. Inicializar predecessor como None por ahora
                self.predecessor = None
                
                # 5. Actualizar finger table
                self._update_finger_table()
                
                # 6. Marcar como unido
                self.is_joined = True
                
                # 7. Iniciar hilos de mantenimiento
                self._start_maintenance_threads()
                
                logger.info(f"Unión exitosa. Successor: {succ_id[:8]}... ({succ_ip}:{succ_port})")
                return True
                
        except Exception as e:
            logger.error(f"Error al unirse al anillo: {e}")
        
        return False
    

    """_find_successor_remote
    descripcion: Encuentra el successor de una clave contactando un nodo remoto.
    entrada: key_id hash de la clave, target_ip IP del nodo remoto, target_port puerto del nodo remoto
    salida: (ip, port, node_id) del successor o None
    """ 
    def _find_successor_remote(self, key_id: str, target_ip: str, target_port: int) -> Optional[Tuple[str, int, str]]:
        """Encuentra el successor contactando un nodo remoto (VERSIÓN REAL)"""
        logger.debug(f"Buscando successor para clave {key_id[:8]}... en {target_ip}:{target_port}")
        
        if not self.send_callback:
            logger.error("No hay callback configurado")
            return (target_ip, target_port, self._calculate_hash(f"{target_ip}:{target_port}"))
        
        # Enviar mensaje CHORD
        message = {
            "type": "CHORD_FIND_SUCCESSOR",
            "key_id": key_id,
            "requester_id": self.node_id
        }
        
        try:
            self.send_callback(target_ip, target_port, message)
            # Por ahora retornamos el nodo target (simplificación)
            return (target_ip, target_port, self._calculate_hash(f"{target_ip}:{target_port}"))
        except Exception as e:
            logger.error(f"Error contactando {target_ip}:{target_port}: {e}")
            return None

    """_notify_successor
    descripcion: Notifica al successor que podríamos ser su nuevo predecessor.
     entrada: succ_ip IP del successor, succ_port puerto del successor
    salida: -
    """
    def _notify_successor(self, succ_ip: str, succ_port: int):
        """Notifica al successor (VERSIÓN REAL)"""
        logger.debug(f"Notificando a {succ_ip}:{succ_port} como possible predecessor")
        
        if not self.send_callback:
            return
        
        message = {
            "type": "CHORD_NOTIFY",
            "node_id": self.node_id,
            "ip": self.ip,
            "port":  self.port
        }
        
        try: 
            self.send_callback(succ_ip, succ_port, message)
        except Exception as e:
            logger.error(f"Error notificando a {succ_ip}:{succ_port}: {e}")

    """find_successor
    descripcion: Encuentra el nodo responsable de una clave en el anillo.   
    entrada: key_id hash de la clave a buscar
    salida: (ip, port, node_id) del nodo responsable, o None
    """
    def find_successor(self, key_id: str) -> Optional[Tuple[str, int, str]]:
        if not self.is_joined:
            logger.warning("Nodo no unido al anillo")
            return None
        
        # 1. Si la clave está entre nosotros y nuestro successor
        if self.successor and self._is_between(
            key_id, 
            self.node_id, 
            self.successor[2],  # node_id del successor
            inclusive=True  # Incluyendo al successor
        ):
            return self.successor
        
        # 2. Buscar en finger table el nodo más cercano
        closest = self._closest_preceding_node(key_id)
        
        if closest:
            # 3. Contactar a ese nodo recursivamente
            # (En versión real: enviar mensaje FIND_SUCCESSOR)
            ip, port, node_id = closest
            # Por ahora, simplificamos
            return closest
        
        # 4. Fallback al successor
        return self.successor
    

    """_closest_preceding_node 
    descripcion: Encuentra en la finger table el nodo con ID más grande pero menor que key_id. (para busqueda de responsables de key)
    entrada: key_id hash de la clave
     salida: (ip, port, node_id) del nodo encontrado o None
    """
    def _closest_preceding_node(self, key_id: str) -> Optional[Tuple[str, int, str]]:
        # Versión simplificada: usar finger table si existe
        for node in reversed(self.finger_table):  # Empezar por los más lejanos
            if self._is_between(node[2], self.node_id, key_id, inclusive=False):
                return node
        
        return None
    
    # ==================== MANTENIMIENTO DEL ANILLO ====================
    
    """_start_maintenance_threads
    descripcion: Inicia hilos para mantenimiento periódico del anillo.
    entrada: -
    salida: -
    """
    def _start_maintenance_threads(self):
        self.stabilize_thread = threading.Thread(target=self._stabilize_loop, daemon=True)
        self.fix_fingers_thread = threading.Thread(target=self._fix_fingers_loop, daemon=True)
        self.check_predecessor_thread = threading.Thread(target=self._check_predecessor_loop, daemon=True)
        
        self.stabilize_thread.start()
        self.fix_fingers_thread.start()
        self.check_predecessor_thread.start()
        
        logger.info("Hilos de mantenimiento iniciados")
    
    """_stabilize_loop
    descripcion: Bucle de estabilización periódica.
    entrada: -
    salida: -"""
    def _stabilize_loop(self):
        while self.running and self.is_joined:
            try:
                self._stabilize()
            except Exception as e:
                logger.error(f"Error en stabilize: {e}")
            
            time.sleep(10)
    
    """_stabilize
    descripcion: Rutina de estabilización Chord.
    entrada: -
    salida: -"""
    def _stabilize(self):
        if not self.successor:
            return
        
        succ_ip, succ_port, succ_id = self.successor
        
        try:
            # 1. Pedir predecessor del successor (simulado)
            # pred = self._ask_predecessor(succ_ip, succ_port)
            pred = None  # Por ahora, placeholder
            
            if pred and self._is_between(pred[2], self.node_id, succ_id, inclusive=False):
                # 2. Si el predecessor del successor está entre nosotros y el successor
                # actualizamos nuestro successor
                self.successor = pred
                logger.info(f"Successor actualizado: {pred[2][:8]}...")
            
            # 3. Notificar al successor sobre nosotros
            self._notify_successor(succ_ip, succ_port)
            
        except Exception as e:
            logger.warning(f"No se pudo contactar successor {succ_id[:8]}...: {e}")
    
    """_fix_fingers_loop
    descripcion: Bucle para actualizar periódicamente la finger table.
    entrada: -
    salida: -"""
    def _fix_fingers_loop(self):
        while self.running and self.is_joined:
            try:
                self._update_finger_table()
            except Exception as e:
                logger.error(f"Error actualizando finger table: {e}")
            
            time.sleep(30)
    
    """_update_finger_table
    descripcion: Actualiza la finger table del nodo.
    entrada: -
    salida: -"""
    def _update_finger_table(self):
        if not self.successor:
            return
        
        # Versión simplificada: finger table con los próximos 3 sucesores
        self.finger_table = []
        
        current = self.successor
        for i in range(3):  # Solo 3 entradas para Chord reducido
            if current:
                self.finger_table.append(current)
                # En versión real: pedir successor de current
                # current = self._get_successor_of(current[0], current[1])
    
    """_check_predecessor_loop
    descripcion: Bucle para verificar si el predecessor sigue activo. Esto para rearmar el chord de ser necesario.
    entrada: -
    salida: -"""
    def _check_predecessor_loop(self):
        while self.running and self.is_joined:
            if self.predecessor:
                pred_ip, pred_port, pred_id = self.predecessor
                # En versión real: enviar HEARTBEAT
                # Si no responde: self.predecessor = None
                pass
            
            time.sleep(15)
    
    # ==================== API PÚBLICA ====================
    
    """get_node_info
    descripcion: Retorna información del nodo para debugging.
    entrada: -
    salida: Diccionario con información del nodo"""
    def get_node_info(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "successor": self.successor[2] if self.successor else None,
            "predecessor": self.predecessor[2] if self.predecessor else None,
            "is_joined": self.is_joined,
            "finger_table_size": len(self.finger_table)
        }
    
    """get_responsible_node
    descripcion: Determina qué nodo es responsable de una clave.
    entrada: key Clave a buscar (string) 
    salida: (ip, port, node_id) del nodo responsable sino None
    """
    def get_responsible_node(self, key: str) -> Optional[Tuple[str, int, str]]:
        key_hash = self._calculate_hash(key)
        return self.find_successor(key_hash)
    
    """leave_network
    descripcion: Abandona el anillo de manera ordenada.
    entrada: -
    salida: -"""
    def leave_network(self):
        logger.info("Saliendo del anillo...")
        self.running = False
        
        # 1. Notificar a predecessor y successor
        if self.predecessor and self.successor:
            # En versión real: notificar para que se conecten entre ellos
            logger.debug("Notificando salida a vecinos")
        
        # 2. Limpiar estado
        self.successor = None
        self.predecessor = None
        self.is_joined = False
        
        logger.info("Nodo ha salido del anillo")
    
    # ==================== MANEJO DE MENSAJES ====================
    
    def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Procesa mensajes entrantes para el overlay.
        entrada: message Diccionario con el mensaje recibido
        salida:  Diccionario con la respuesta o None
        """
        msg_type = message.get("type")
        
        # ⭐ AGREGAR LOS NOMBRES CORRECTOS (con prefijo CHORD_)
        handlers = {
            "CHORD_JOIN_REQUEST": self._handle_join_request,
            "CHORD_FIND_SUCCESSOR": self._handle_find_successor,
            "CHORD_NOTIFY": self._handle_notify,  # ⭐ AGREGAR ESTE
            "CHORD_UPDATE_PREDECESSOR": self._handle_update_predecessor,
            "CHORD_HEARTBEAT": self._handle_heartbeat,
            # También mantener los nombres sin prefijo por compatibilidad
            "JOIN_REQUEST": self._handle_join_request,
            "FIND_SUCCESSOR": self._handle_find_successor,
            "UPDATE_PREDECESSOR": self._handle_update_predecessor,
            "HEARTBEAT": self._handle_heartbeat,
        }
        
        handler = handlers.get(msg_type)
        if handler:
            return handler(message)
        
        logger.warning(f"Mensaje no manejado: {msg_type}")
        return None
    
    """_handle_join_request
    descripcion: Maneja solicitud de unión de nuevo nodo.
    entrada: message Diccionario con el mensaje JOIN_REQUEST
    salida: Diccionario con la respuesta JOIN_RESPONSE"""
    def _handle_join_request(self, message: Dict) -> Dict:
        new_node_id = message.get("node_id")
        new_ip = message.get("ip")
        new_port = message.get("port")
        
        logger.info(f"Procesando JOIN_REQUEST de {new_node_id[:8]}... ({new_ip}:{new_port})")
        
        # Encontrar successor para el nuevo nodo
        succ = self.find_successor(new_node_id)
        
        response = {
            "type": "JOIN_RESPONSE",
            "successor_ip": succ[0] if succ else None,
            "successor_port": succ[1] if succ else None,
            "successor_id": succ[2] if succ else None,
        }
        
        return response
    
    """_handle_find_successor
    descripcion: Maneja búsqueda de successor.
    entrada: message Diccionario con el mensaje FIND_SUCCESSOR
    salida: Diccionario con la respuesta SUCCESSOR_RESPONSE"""
    def _handle_find_successor(self, message: Dict) -> Dict:
        key_id = message.get("key_id")
        succ = self.find_successor(key_id)
        
        response = {
            "type": "SUCCESSOR_RESPONSE",
            "key_id": key_id,
            "successor_ip": succ[0] if succ else None,
            "successor_port": succ[1] if succ else None,
            "successor_id": succ[2] if succ else None,
        }
        
        return response
    
    """_handle_update_predecessor
    descripcion: Actualiza el predecessor.
    entrada: message Diccionario con el mensaje UPDATE_PREDECESSOR
    salida: Diccionario con la respuesta ACK"""
    def _handle_update_predecessor(self, message: Dict) -> Dict:
        new_pred_id = message.get("node_id")
        new_pred_ip = message.get("ip")
        new_pred_port = message.get("port")
        
        # Solo actualizar si es nuestro predecessor válido
        if self._is_between(new_pred_id, self.predecessor[2] if self.predecessor else "0", 
                           self.node_id, inclusive=True):
            self.predecessor = (new_pred_ip, new_pred_port, new_pred_id)
            logger.info(f"Predecessor actualizado: {new_pred_id[:8]}...")
        
        return {"type": "ACK"}
    
    """_handle_heartbeat
    descripcion: Responde a heartbeat.
    entrada: message Diccionario con el mensaje HEARTBEAT
    salida: Diccionario con la respuesta HEARTBEAT_ACK"""
    def _handle_heartbeat(self, message: Dict) -> Dict:
        return {
            "type": "HEARTBEAT_ACK",
            "timestamp": time.time()
        }

    def _handle_notify(self, message: Dict) -> Optional[Dict]:
        """
        _handle_notify
        descripcion: Maneja notificación de posible nuevo predecessor
        entrada: message Diccionario con el mensaje CHORD_NOTIFY
        salida: None (no requiere respuesta)
        """
        new_pred_id = message.get("node_id")
        new_pred_ip = message.get("ip")
        new_pred_port = message.get("port")
        
        logger.info(f"📢 NOTIFY recibido de {new_pred_id[: 8]}...  ({new_pred_ip}:{new_pred_port})")
        
        # Actualizar predecessor si: 
        # 1. No tenemos predecessor, O
        # 2. El nuevo nodo está más cerca de nosotros que el predecessor actual
        
        if not self.predecessor:
            # No tenemos predecessor, aceptar este
            self.predecessor = (new_pred_ip, new_pred_port, new_pred_id)
            logger.info(f"✅ Predecessor establecido: {new_pred_id[:8]}...")
            return None
        
        # Verificar si el nuevo nodo está entre el predecessor actual y nosotros
        if self._is_between(new_pred_id, self.predecessor[2], self.node_id, inclusive=False):
            self.predecessor = (new_pred_ip, new_pred_port, new_pred_id)
            logger.info(f"✅ Predecessor actualizado: {new_pred_id[:8]}...")
        else:
            logger.debug(f"Ignorando NOTIFY de {new_pred_id[:8]}...  (no es mejor predecessor)")
        
        return None  # NOTIFY no requiere respuesta

# ==================== FUNCIONES DE UTILIDAD ====================

"""create_node_id
    descripcion: Crea un ID de nodo basado en IP y puerto.
    entrada: ip Dirección IP del nodo, port Puerto del nodo
    salida: string con el hash SHA-1 del nodo
    """
def create_node_id(ip: str, port: int) -> str:
    """Crea un ID de nodo basado en IP y puerto."""
    node_string = f"{ip}:{port}"
    return hashlib.sha1(node_string.encode()).hexdigest()

"""is_key_in_range
    descripcion: Determina si un hash está en un rango en el anillo circular.
    entrada: key_hash Hash a verificar, range_start Inicio del rango, range_end Fin del rango, inclusive_end Si el fin es inclusivo
    salida: Booleano indicando si el hash está en el rango"""
def is_key_in_range(key_hash: str, range_start: str, range_end: str, inclusive_end: bool = True) -> bool:
    k = int(key_hash, 16)
    s = int(range_start, 16)
    e = int(range_end, 16)
    
    if s < e:
        if inclusive_end:
            return s < k <= e
        return s < k < e
    else:
        if inclusive_end:
            return s < k or k <= e
        return s < k or k < e


# ==================== EJEMPLO DE USO ====================
"""
if __name__ == "__main__":
    #Ejemplo mínimo para probar el overlay.

    print("=== Testing Chord Overlay ===")
    
    # 1. Crear primer nodo (inicia el anillo)
    node1 = ChordNode("192.168.1.100", 5000)
    print(f"Nodo 1 creado: {node1.node_id[:8]}")
    print(f"Info: {node1.get_node_info()}")
    
    # 2. Crear segundo nodo que se une al primero
    node2 = ChordNode("192.168.1.101", 5001, ("192.168.1.100", 5000))
    print(f"\nNodo 2 creado: {node2.node_id[:8]}")
    print(f"Info: {node2.get_node_info()}")
    
    # 3. Buscar nodo responsable para una clave
    test_key = "mi_clave_secreta"
    responsible = node1.get_responsible_node(test_key)
    if responsible:
        ip, port, node_id = responsible
        print(f"\nPara clave '{test_key}' -> Nodo responsable: {node_id[:8]} ({ip}:{port})")
    
    # 4. Simular mensajes
    join_msg = {
        "type": "JOIN_REQUEST",
        "node_id": create_node_id("192.168.1.102", 5002),
        "ip": "192.168.1.102",
        "port": 5002
    }
    
    response = node1.handle_message(join_msg)
    print(f"\nRespuesta a JOIN_REQUEST: {response}")
    
    # 5. Salir ordenadamente
    node2.leave_network()
    print("\nPrueba completada")
    """