#MODULO 3: OVERLAY - CHORD / Anillo hash
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


#clase chordnode: mantiene successor, predecessor y tabla de finger simplificada

class ChordNode:

    """ __init__
    descripcion: Inicializa un nuevo nodo Chord
    entrada: ip Dirección IP del nodo, port Puerto del nodo, 
    existing_node (ip, port) de un nodo existente para unirse al anillo
    salida: - """
    def __init__(self, ip: str, port: int, existing_node:  Tuple[str, int] = None, send_callback = None):
        self.ip = ip
        self.port = port
        self.send_callback = send_callback  
        self.request_callback = None
        # Mapa de vecinos conocidos por node_id → (ip, port)
        self.neighbors: Dict[str, Tuple[str, int]] = {}

        
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
    


    
    #  FUNCIONES HASH 
    """_calculate_hash
    descripcion: calcula el hash SHA-1 de una cadena. Convierte el string a bytes, hashlib calcula el SHA-1 y hexdigest convierte el resultado
    hexadecimal.
    entrada: key cadena que se debe hashear
    salida: string de hash hexadecimal de 40 caracteres """
    def _calculate_hash(self, key: str) -> str:
        return hashlib.sha1(key.encode()).hexdigest()
    
    
    """is_between
    descripcion: Verifica si una clave está en cierto intervalo en el anillo.
    entrada: key hash a verificar, start inicio del intervalo, end fin del intervalo, inclusive si end es inclusivo
    salida: booleano indicando si key está en el intervalo"""  
    def _is_between(self, key: str, start: str, end: str, inclusive: bool = True) -> bool:
        key_int = int(key, 16)
        start_int = int(start, 16)
        end_int = int(end, 16)
        
        #Caso en el que no damos la vuelta el anillo, no vamos a 0
        if start_int < end_int:
            #inclusive, end está incluido en el intervalo, valor necesario para evitar duplicados
            if inclusive:
                return start_int < key_int <= end_int
            return start_int < key_int < end_int
        
        #Caso en el que damos la vuelta al anillo, Por ejemplo estamos en 50 y queremos llegar a 10, pasamos por el 0
        elif start_int > end_int:
            if inclusive:
                return start_int < key_int or key_int <= end_int
            return start_int < key_int or key_int < end_int
        
        else:  # start == end
            if inclusive:
                return key_int == start_int
            return False
            

    """set_send_callback
    descripcion: Configura la función callback para enviar mensajes. Que enviar y a quien
    entrada: callback función que envía mensajes (ip, port, message)
    salida: -"""      
    def set_send_callback(self, callback):
        self.send_callback = callback
    

    """set_request_callback
    descripcion: Configura la función callback para request/response síncrono.
    entrada: callback función que maneja request/response (ip, port, message) -> response dict
    salida: -"""
    def set_request_callback(self, callback):
        """Configura un callback síncrono (ip, port, message) -> respuesta dict."""
        self.request_callback = callback

    """_remember_node
    descripcion: Guarda en el mapa de vecinos si hay datos suficientes.
    entrada: node_id ID del nodo, ip Dirección IP del nodo, port Puerto del nodo
    salida: -"""
    def _remember_node(self, node_id: Optional[str], ip: Optional[str], port: Optional[int]):
        """Guarda en el mapa de vecinos si hay datos suficientes."""
        try:
            if node_id and ip and port is not None:
                self.neighbors[node_id] = (ip, int(port))
        except Exception:
            pass
    

    #  OPERACIONES DEL ANILLO 
    """join_network
    descripcion: Une este nodo al anillo Chord usando un nodo existente como punto de referencia. Entra un nodo no conectadoa a nadie.
    entrada: existing_node (ip, port) de un nodo existente
    salida: -"""
    def join_network(self, existing_node: Tuple[str, int]):
        existing_ip, existing_port = existing_node
        
        #informacion para debug
        logger.info(f"Uniéndose al anillo a través de {existing_ip}:{existing_port}")
        
        try:
            # se contacta al nodo existente para encontrar el sucesor del nuevo nodo
            successor = self._find_successor_remote(self.node_id, existing_ip, existing_port)
            
            if successor:
                # asigna al sucesor
                succ_ip, succ_port, succ_id = successor
                self.successor = (succ_ip, succ_port, succ_id)
                self._remember_node(succ_id, succ_ip, succ_port)
                
                # notificar al sucesor que somos su posible (puede ser momentaneo) predecesor
                self._notify_successor(succ_ip, succ_port)
                
                # se inicializa predecesor como none por el momento ya que se va a actualizar luego
                self.predecessor = None
                
                # calculamos la finger table inicial según la posición en que estamos
                self._update_finger_table()
                
                # marcar al nodo como unido al anillo
                self.is_joined = True
                
                # inciia el mantenimiento periódico del anillo para reconstruir finger table, estabilizar, etc
                self._start_maintenance_threads()
                
                # informacion para debug 
                logger.info(f"Unión exitosa. Successor: {succ_id[:8]}... ({succ_ip}:{succ_port})")
                return True
                
        except Exception as e:
            logger.error(f"Error al unirse al anillo: {e}")
        
        return False
    

    """_find_successor_remote
    descripcion: Encuentra el successor de una clave contactando un nodo remoto.
    entrada: key_id hash de la clave, target_ip IP del nodo remoto, target_port puerto del nodo remoto
    salida: (ip, port, node_id) del successor o None""" 
    def _find_successor_remote(self, key_id: str, target_ip: str, target_port: int) -> Optional[Tuple[str, int, str]]:
        # información para debug de envío de mensajes
        logger.debug(f"Buscando successor para clave {key_id[:8]} en {target_ip}:{target_port}")

        # Construir el mensaje CHORD de consulta
        message = {
            "type": "CHORD_FIND_SUCCESSOR", #tipo de mensaje
            "key_id": key_id, #clave a buscar
            "requester_id": self.node_id, #id del nodo que hace la consulta
        }

        # camino sincrono
        if self.request_callback:
            try:
                response = self.request_callback(target_ip, target_port, message)
                if response and response.get("type") == "SUCCESSOR_RESPONSE":
                    ip = response.get("successor_ip")
                    port = response.get("successor_port")
                    node_id = response.get("successor_id")
                    if ip and port and node_id:
                        self._remember_node(node_id, ip, port)
                        return (ip, port, node_id)
                # en caso de que la respuesta no es válida
                logger.warning("Respuesta inválida o incompleta al buscar successor remoto")
            except Exception as e:
                logger.error(f"Error en request/response con {target_ip}:{target_port}: {e}")

        # envío asíncrono, asumir el target como candidato
        if self.send_callback:
            try:
                self.send_callback(target_ip, target_port, message)
            except Exception as e:
                logger.error(f"Error contactando {target_ip}:{target_port}: {e}")
                return None

        # En última instancia, devolver el propio target como candidato (heurística)
        return (target_ip, target_port, self._calculate_hash(f"{target_ip}:{target_port}"))


    """_notify_successor
    descripcion: Notifica al successor que podríamos ser su nuevo predecessor.
     entrada: succ_ip IP del successor, succ_port puerto del successor
    salida: -"""
    def _notify_successor(self, succ_ip: str, succ_port: int):
        logger.debug(f"Notificando a {succ_ip}:{succ_port} como possible predecessor")
        
        # si no hay función de envío, no se puede notificar
        if not self.send_callback:
            return
        
        message = {
            "type": "CHORD_NOTIFY", #tipo de mensaje
            "node_id": self.node_id, #id del nodo que notifica
            "ip": self.ip, #ip del nodo que notifica, donde se encuentra
            "port":  self.port #puerto del nodo que notifica
            ,"timestamp": time.time(),
            "current_predecessor": (  # informacion útil para debug
            self.predecessor[2] if self.predecessor else None
            )
        }
        
        try: 
            self.send_callback(succ_ip, succ_port, message)
            logger.debug(f"NOTIFY enviado a {succ_ip}:{succ_port}")
        except Exception as e:
            logger.error(f"Error notificando a {succ_ip}:{succ_port}: {e}")


    """find_successor
    descripcion: encuentra el nodo responsable de una clave en el anillo.   
    entrada: key_id hash de la clave a buscar
    salida: (ip, port, node_id) del nodo responsable, o None"""
    def find_successor(self, key_id: str) -> Optional[Tuple[str, int, str]]:
        # Si aún no está unido, permitir fallback si ya se auto-referenció como successor
        if not self.is_joined:
            logger.warning("Nodo no unido al anillo")
            return None
        
        # verificamos si la clave está entre nosotros (nodo actual) y nuestro successor
        if self.successor and self._is_between(
            key_id, 
            self.node_id,  #mi id
            self.successor[2],  # id del successor
            inclusive=True  # incluir al successor
        ):
            logger.debug(f"Clave {key_id[:8]}... está en mi segmento")
            return self.successor #indicar que el successor es el responsable por lo tanto nodo actual es predecesor
        
        # buscamos en la finger table el nodo mas cercano que sea menor a la llave que buscamos
        closest = self._closest_preceding_node(key_id)
        
        if closest:
            # Intentar buscar remotamente
            result = self._find_successor_remote(key_id, closest[0], closest[1])
            if result:
                return result
                
        # === CORRECCIÓN ===
        # Si la búsqueda remota falla o no hay fingers, usar el sucesor actual
        if self.successor:
            return self.successor
                    
        # Fallback final: Si no conozco a nadie, soy yo mismo
        return (self.ip, self.port, self.node_id)
    

    """_closest_preceding_node 
    descripcion: Encuentra en la finger table el nodo con ID más grande pero menor que key_id. 
    entrada: key_id hash de la clave
     salida: (ip, port, node_id) del nodo encontrado o None"""
    def _closest_preceding_node(self, key_id: str) -> Optional[Tuple[str, int, str]]:
        for node in reversed(self.finger_table):  # empieza por los más lejanos
            if self._is_between(node[2], self.node_id, key_id, inclusive=False): #verifica si el nodo está entre nosotros y la clave
                #si es asi retornar el nodo
                return node
        #si no se encuentra ninguno, retornar none
        return None
    



    # FUNCIONES DE MANTENIMIENTO DEL ANILLO 
    
    """_start_maintenance_threads
    descripcion: Inicia hilos para mantenimiento periódico del anillo.
    entrada: -
    salida: - """
    def _start_maintenance_threads(self):
        #crea un hilo para estabilizar el anillo periódicamente
        self.stabilize_thread = threading.Thread(target=self._stabilize_loop, daemon=True)
        #crea un hilo para actualizar la finger table periódicamente
        self.fix_fingers_thread = threading.Thread(target=self._fix_fingers_loop, daemon=True)
        #crea un hilo para verificar si el predecessor sigue activo, si no lo está, se limpia y se reconfigura
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
                # === CORRECCIÓN: AUTO-CURACIÓN ===
                if not self.successor: 
                    logger.warning("No hay successor. Intentando recuperar conexión...")
                            
                # Intento 1: Usar el predecesor si existe (vuelta atrás)
                    if self.predecessor:
                        logger.info(f"Recuperando usando predecesor {self.predecessor[2][:8]}...")
                        self.successor = self.predecessor
                            
                # Intento 2: Usar algún vecino conocido de la lista
                    elif self.neighbors:
                         # Tomamos el primero que encontremos
                        nid, (nip, nport) = next(iter(self.neighbors.items()))
                        logger.info(f"Recuperando usando vecino conocido {nid[:8]}...")
                        self.successor = (nip, nport, nid)
                            
                    else:
                        # No hay nada que hacer, esperar
                        time.sleep(1)
                        continue
                
                succ_ip, succ_port, succ_id = self.successor
                
                # Caso especial: si mi successor soy yo mismo.
                if succ_id == self.node_id:
                    # Busca si hay otro nodo
                    if self.predecessor and self.predecessor[2] != self.node_id:
                        # Hay otro nodo:  hacerlo mi successor
                        logger.info(f"Stabilize: Cambiando successor de mí mismo a {self.predecessor[2][:8]}...")
                        self.successor = self.predecessor
                        time.sleep(1)
                        continue
                    else:
                        #Caso en el que no haya nadie más
                        logger.debug("Stabilize: Anillo de 1 nodo")
                        time.sleep(1)
                        continue
                
                # Preguntar al successor: "¿Quién es tu predecessor?"
                logger.debug(f"Stabilize: Preguntando predecessor a {succ_id[:8]}...")
                
                if not self.send_callback: 
                    logger.warning("No hay send_callback configurado")
                    time.sleep(1)
                    continue
                
                # Envia el mensaje a "GET_PREDECESSOR"
                message = {
                    "type": "CHORD_GET_PREDECESSOR",
                    "requester_id": self.node_id,
                    "requester_ip": self.ip,
                    "requester_port": self.port,
                    "timestamp": time.time()
                }
                
                try:
                    response = None
                    if self.request_callback:
                        try:
                            response = self.request_callback(succ_ip, succ_port, message)
                        except Exception as e:
                            logger.warning(f"Fallo comunicación en stabilize: {e}")
                    if response:
                        #Obtiene la información necesaria.
                        pred_ip = response.get("predecessor_ip")
                        pred_port = response.get("predecessor_port")
                        pred_id = response.get("predecessor_id")
                        
                        if pred_ip and pred_port and pred_id:
                            # Verificar si ese predecessor está entre yo y mi successor
                            if self._is_between(pred_id, self.node_id, succ_id, inclusive=False):
                                # Ese nodo debería ser mi successor
                                old_succ = self.successor
                                self.successor = (pred_ip, pred_port, pred_id)
                                logger. info(f"Successor actualizado por stabilize correctamente: {old_succ[2][:8]}...  → {pred_id[:8]}...")
                            else:
                                logger.debug(f"Stabilize:  Predecessor {pred_id[:8]}... no está entre yo y successor")
                        else:
                            # El successor no tiene predecessor (o es None)
                            logger.debug("Stabilize: Successor no tiene predecessor")
                    
                except Exception as e:
                    logger.debug(f"Stabilize: Error preguntando a successor: {e}")
                
                # Notificar al successor que existo (podría ser su nuevo predecessor)
                notify_msg = {
                    "type": "CHORD_NOTIFY",
                    "node_id": self.node_id,
                    "ip":  self.ip,
                    "port": self.port,
                    "timestamp": time.time(),
                }
                
                try:
                    self. send_callback(succ_ip, succ_port, notify_msg)
                    logger.debug(f"Stabilize:  NOTIFY enviado a {succ_id[:8]}...")
                except Exception:
                    pass
            except Exception as e: 
                logger.error(f"Error en stabilize loop: {e}")
            
            time.sleep(1)

    """_ask_predecessor_of_successor
    descripcion: Pregunta al successor quién es su predecessor.
    entrada: succ_ip IP del successor, succ_port puerto del successor
    salida: (ip, port, node_id) del predecessor, o None"""
    def _ask_predecessor_of_successor(self, succ_ip: str, succ_port: int) -> Optional[Tuple[str, int, str]]:
        if not self.send_callback:
            logger.warning("No hay callback para preguntar predecesor")
            return None
        
        # mensaje para preguntar por predecesor
        message = {
            "type": "CHORD_GET_PREDECESSOR",
            "requester_id": self.node_id, #id del nodo que hace la consulta
            "requester_ip": self.ip, #ip del nodo que hace la consulta
            "requester_port": self.port, #puerto del nodo que hace la consulta
            "timestamp": time.time() 
        }
        
        try:
            # usar request_callback si existe (para respuesta síncrona)
            if self.request_callback:
                response = self.request_callback(succ_ip, succ_port, message)
                if (response and 
                    response.get("type") == "PREDECESSOR_RESPONSE" and
                    response.get("predecessor_id") is not None):
                    
                    pred_id = response["predecessor_id"]
                    pred_ip = response.get("predecessor_ip", succ_ip)  # Fallback
                    pred_port = response.get("predecessor_port", succ_port)
                    
                    return (pred_ip, pred_port, pred_id)
            
            # Si no hay request_callback, usar send_callback y simular
            self.send_callback(succ_ip, succ_port, message)
            logger.debug(f"Preguntado predecesor a {succ_ip}:{succ_port}")
            
            # Por ahora, simular que no tiene predecesor
            # (En implementación real, esperarías respuesta)
            return None
            
        except Exception as e:
            logger.error(f"Error preguntando predecesor a {succ_ip}:{succ_port}: {e}")
            return None

    """_stabilize
    descripcion: Rutina de estabilización Chord.
    entrada: -
    salida: -"""
    def _stabilize(self):
        # Si no tenemos sucesor, no hay que estabilizar
        if not self.successor:
            logger.debug("No hay successor para estabilizar")
            return
        
        succ_ip, succ_port, succ_id = self.successor

        if succ_id == self.node_id:
            logger.error("ERROR: Successor es self. Corrigiendo a None")
            self.successor = None
            return 
    
        logger.debug(f"Ejecutando stabilize con successor: {succ_id[:8]}...")
        
        try:
            # preguntar al sucesor su predecesor
            pred_of_successor = self._ask_predecessor_of_successor(succ_ip, succ_port)
            
            # verificar si debemos actualizar nuestro sucesor
            if pred_of_successor:
                pred_ip, pred_port, pred_id = pred_of_successor
                
                # verificar si el predecesor del sucesor está entre nosotros y nuestro sucesor
                if self._is_between(pred_id, self.node_id, succ_id, inclusive=False):
                    # actualizar al sucesor
                    logger.info(f"Encontrado mejor successor: {pred_id[:8]}... "
                            f"(estaba: {succ_id[:8]}...)")
                    
                    self.successor = (pred_ip, pred_port, pred_id)
                    
                    # actualizar la finger table
                    self._update_finger_table()
            
            # notificar al sucesor que somos su posible predecesor
            self._notify_successor(self.successor[0], self.successor[1])
            
            logger.debug("Stabilize completado exitosamente")
                
        except Exception as e:
            logger.error(f"Error en stabilize con {succ_id[:8]}...: {e}")


    """_fix_fingers_loop
    descripcion: Bucle para actualizar periódicamente la finger table.
    entrada: -
    salida: -"""
    def _fix_fingers_loop(self):
        #verificamos si el nodo sigue corriendo y está unido al anillo
        while self.running and self.is_joined:
            try:
                # actualizamos la finger table
                self._update_finger_table()
            except Exception as e:
                logger.error(f"Error actualizando finger table: {e}")
            
            time.sleep(30)
    


    """_update_finger_table
    descripcion: Actualiza la finger table del nodo.
    entrada: -
    salida: -"""
    def _update_finger_table(self):
        self.finger_table = []
        m = 160  # bits de SHA-1
        k = 16   # reducir tamaño para laboratorio
        for i in range(1, k + 1):
            try:
                offset = pow(2, i - 1)
                target_id = (int(self.node_id, 16) + offset) % pow(2, m)
                target_id_hex = format(target_id, '040x')
                succ = self.find_successor(target_id_hex)
                if succ:
                    # evitar duplicados consecutivos
                    if not self.finger_table or self.finger_table[-1][2] != succ[2]:
                        self.finger_table.append(succ)
                        self._remember_node(succ[2], succ[0], succ[1])
            except Exception as e:
                logger.debug(f"Error parcial actualizando finger[{i}]: {e}")
    


    """_check_predecessor_loop
    descripcion: Bucle para verificar si el predecessor sigue activo usando HEARTBEATS. Esto para rearmar el chord de ser necesario.
    entrada: -
    salida: -"""
    def _check_predecessor_loop(self):
        # contador de fallos consecutivos
        consecutive_failures = 0
        max_failures = 3  # 3 fallos = nodo caído
        
        while self.running and self.is_joined:
            try:
                if self.predecessor:
                    pred_ip, pred_port, pred_id = self.predecessor
                    
                    logger.debug(f"Verificando predecesor {pred_id[:8]}...")
                    
                    # enviar heartbeat
                    heartbeat_sent = self._send_heartbeat(pred_ip, pred_port)
                    
                    if heartbeat_sent:
                        # esperar respuesta
                        response_received = self._wait_for_heartbeat_ack(pred_id, timeout=5)
                        
                        if response_received:
                            # resetear contador de fallos porque respondió
                            consecutive_failures = 0 
                            logger.debug(f"Predecesor {pred_id[:8]}... responde")
                        else:
                            # incrementar contador de fallos
                            consecutive_failures += 1
                            logger.warning(f"Predecesor {pred_id[:8]}... no respondió "
                                        f"(fallo #{consecutive_failures})")
                            
                            # si se alcanzan los fallos máximos, marcar como caído
                            if consecutive_failures >= max_failures:
                                logger.error(f"Predecesor {pred_id[:8]}... CAÍDO "
                                        f"({consecutive_failures} fallos)")
                                self._handle_predecessor_failure()
                                consecutive_failures = 0
                    else:
                        # error enviando heartbeat
                        logger.warning(f"No se pudo enviar heartbeat a {pred_ip}:{pred_port}")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error en _check_predecessor_loop: {e}")
                time.sleep(2)  # esperar antes de reintentar  




    #  API PÚBLICA 
    
    """get_node_info
    descripcion: Retorna información del nodo para debugging.
    entrada: -
    salida: Diccionario con información del nodo"""
    def get_node_info(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id, #id del nodo
            "ip": self.ip, #ip del nodo
            "port": self.port, #puerto del nodo
            "successor": self.successor[2] if self.successor else None, #id del successor
            "predecessor": self.predecessor[2] if self.predecessor else None, #id del predecessor
            "is_joined": self.is_joined, #si está unido al anillo
            "finger_table_size": len(self.finger_table) #tamaño de la finger table
        }
    
    """get_responsible_node
    descripcion: Determina qué nodo es responsable de una clave.
    entrada: key Clave a buscar (string) 
    salida: (ip, port, node_id) del nodo responsable sino None"""
    def get_responsible_node(self, key: str) -> Optional[Tuple[str, int, str]]:
        key_hash = self._calculate_hash(key) #calcula el hash de la clave
        return self.find_successor(key_hash) #busca el successor responsable de esa clave
    
    """leave_network
    descripcion: Abandona el anillo de manera ordenada.
    entrada: -
    salida: -"""
    def leave_network(self, graceful: bool = True):
        # información para debug
        logger.info(f"{'Saliendo' if graceful else 'Forzando salida'} del anillo...")
        
        # detener mantenimiento
        self.running = False
        
        if graceful:
            # notificar al predecessor y successor
            self._notify_leave()
            
        
        # limpiar estructuras
        self.successor = None
        self.predecessor = None
        self.is_joined = False
        self.finger_table = []
        
        logger.info("Nodo ha salido del anillo")


    """_notify_leave
    descripcion: Notifica al predecessor y successor sobre la salida del nodo.  
    entrada: -
    salida: -"""
    def _notify_leave(self):
        
        # notificar al predecesor que su nuevo sucesor es el sucesor del nodo actual sucesor
        if self.predecessor and self.successor:
            pred_ip, pred_port, pred_id = self.predecessor
            succ_ip, succ_port, succ_id = self.successor
            
            message_to_pred = {
                "type": "CHORD_UPDATE_SUCCESSOR",  #tipo de mensaje
                "new_successor_ip": succ_ip, #ip del nuevo sucesor
                "new_successor_port": succ_port, #puerto del nuevo sucesor
                "new_successor_id": succ_id, #id del nuevo sucesor
                "leaving_node_id": self.node_id, #id del nodo que se va
                "timestamp": time.time()
            }
            
            try:
                self.send_callback(pred_ip, pred_port, message_to_pred)
                logger.info(f"Notificado predecesor {pred_id[:8]}...")
            except Exception as e:
                logger.error(f"Error notificando predecesor: {e}")
        
        # notificar al sucesor que su nuevo predecesor es el sucesor del nodo actual predecesor
        if self.successor and self.predecessor:
            succ_ip, succ_port, succ_id = self.successor
            pred_ip, pred_port, pred_id = self.predecessor
            
            message_to_succ = {
                "type": "CHORD_UPDATE_PREDECESSOR", #tipo de mensaje
                "new_predecessor_ip": pred_ip, #ip del nuevo predecesor
                "new_predecessor_port": pred_port, #puerto del nuevo predecesor
                "new_predecessor_id": pred_id, #id del nuevo predecesor
                "leaving_node_id": self.node_id, #id del nodo que se va
                "timestamp": time.time()
            }
            
            try:
                self.send_callback(succ_ip, succ_port, message_to_succ)
                logger.info(f"Notificado sucesor {succ_id[:8]}...")
            except Exception as e:
                logger.error(f"Error notificando sucesor: {e}")
    



    # FUNCIONES DE MANEJO DE MENSAJES
    
    """Procesa mensajes entrantes para el overlay.
        entrada: message Diccionario con el mensaje recibido
        salida:  Diccionario con la respuesta o None"""
    def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg_type = message.get("type")
        
        handlers = {
            "CHORD_JOIN_REQUEST": self._handle_join_request,
            "CHORD_FIND_SUCCESSOR": self._handle_find_successor,
            "CHORD_NOTIFY": self._handle_notify, 
            "CHORD_UPDATE_PREDECESSOR": self._handle_update_predecessor,
            "CHORD_UPDATE_SUCCESSOR": self._handle_update_successor,
            "CHORD_HEARTBEAT": self._handle_heartbeat,
            "CHORD_GET_PREDECESSOR": self._handle_get_predecessor,
           
            "JOIN_REQUEST": self._handle_join_request,
            "FIND_SUCCESSOR": self._handle_find_successor,
            "UPDATE_PREDECESSOR": self._handle_update_predecessor,
            "UPDATE_SUCCESSOR": self._handle_update_successor,
            "HEARTBEAT": self._handle_heartbeat,
            "GET_PREDECESSOR": self._handle_get_predecessor,
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
        new_node_id = message.get("node_id") #id del nuevo nodo
        new_ip = message.get("ip") #ip del nuevo nodo
        new_port = message.get("port") #puerto del nuevo nodo
        
        logger.info(f"Procesando JOIN_REQUEST de {new_node_id[:8]}... ({new_ip}:{new_port})")
        
        # recordar vecino
        self._remember_node(new_node_id, new_ip, new_port)

        # encontrar successor para el nuevo nodo
        succ = self.find_successor(new_node_id)

        # caso de anillo vacío o sin successor
        if not succ:
            if self.successor:
                succ = self.successor
            else:
            # Fallback final: Yo soy el sucesor temporal para que entre al anillo
                succ = (self.ip, self.port, self.node_id)
        
        response = {
            "type": "JOIN_RESPONSE",
            "successor_ip": succ[0] if succ else None, #ip del successor
            "successor_port": succ[1] if succ else None, #puerto del successor
            "successor_id": succ[2] if succ else None, #id del successor
        }
        
        return response
    


    """_handle_find_successor
    descripcion: Maneja búsqueda de successor.
    entrada: message Diccionario con el mensaje FIND_SUCCESSOR
    salida: Diccionario con la respuesta SUCCESSOR_RESPONSE"""
    def _handle_find_successor(self, message: Dict) -> Dict:
        key_id = message.get("key_id") #id de la clave a buscar
        succ = self.find_successor(key_id) #buscar el successor de la clave
        if succ:
            self._remember_node(succ[2], succ[0], succ[1])
        
        response = {
            "type": "SUCCESSOR_RESPONSE",
            "key_id": key_id, #id de la clave
            "successor_ip": succ[0] if succ else None, #ip del successor
            "successor_port": succ[1] if succ else None, #puerto del successor
            "successor_id": succ[2] if succ else None, #id del successor
        }
        
        return response
    


    """_handle_update_predecessor
    descripcion: Actualiza el predecessor.
    entrada: message Diccionario con el mensaje UPDATE_PREDECESSOR
    salida: Diccionario con la respuesta ACK"""
    def _handle_update_predecessor(self, message: Dict) -> Dict:
        new_pred_id = message.get("new_predecessor_id") or message.get("node_id") #id del nuevo predecessor
        new_pred_ip = message.get("new_predecessor_ip") or message.get("ip") #ip del nuevo predecessor
        new_pred_port = message.get("new_predecessor_port") or message.get("port") #puerto del nuevo predecessor

        if new_pred_id == self.node_id:
            logger.error("UPDATE_PREDECESSOR rechazado: no puedo ser mi propio predecessor")
            self.predecessor = None
            return {"type": "ACK", "error": "predecessor_cannot_be_self"}

        # validación ligera: si existe predecessor, comprobar intervalo (solo log)
        if self.predecessor and not self._is_between(new_pred_id, self.predecessor[2], self.node_id, inclusive=True):
            logger.debug("UPDATE_PREDECESSOR fuera de intervalo esperado; aplicando de todas formas")

        # actualizar predecessor
        self.predecessor = (new_pred_ip, int(new_pred_port) if new_pred_port is not None else None, new_pred_id)
        self._remember_node(new_pred_id, new_pred_ip, new_pred_port)
        logger.info(f"Predecessor actualizado: {new_pred_id[:8]}...")
        return {"type": "ACK"}


    """_handle_update_successor
    descripcion: Actualiza el successor a partir de un mensaje.
    entrada: message con campos new_successor_* o (node_id, ip, port)
    salida: Diccionario con ACK
    """
    def _handle_update_successor(self, message: Dict) -> Dict:
        new_succ_id = message.get("new_successor_id") or message.get("node_id") #id del nuevo successor
        new_succ_ip = message.get("new_successor_ip") or message.get("ip") #ip del nuevo successor
        new_succ_port = message.get("new_successor_port") or message.get("port") #puerto del nuevo successor

        if new_succ_id == self.node_id:
            logger.error("UPDATE_SUCCESSOR rechazado: no puedo ser mi propio successor")
            self.successor = None
            return {"type": "ACK", "error": "successor_cannot_be_self"}

        # validación ligera: si existe successor, comprobar intervalo (solo log)
        if self.successor and not self._is_between(new_succ_id, self.node_id, self.successor[2], inclusive=True):
            logger.debug("UPDATE_SUCCESSOR fuera de intervalo esperado; aplicando de todas formas")

        # actualizar successor
        self.successor = (new_succ_ip, int(new_succ_port) if new_succ_port is not None else None, new_succ_id)
        self._remember_node(new_succ_id, new_succ_ip, new_succ_port)
        logger.info(f"Successor actualizado: {new_succ_id[:8]}...")
        # actualizar finger table
        try:
            self._update_finger_table()
        except Exception as e:
            logger.error(f"Error actualizando finger table tras UPDATE_SUCCESSOR: {e}")
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


    """_handle_get_predecessor
    descripcion: Responde con el predecessor actual de este nodo.
    entrada: message Diccionario con el mensaje CHORD_GET_PREDECESSOR
    salida: Diccionario con la respuesta PREDECESSOR_RESPONSE
    """
    def _handle_get_predecessor(self, message: Dict) -> Dict:
        # obtener datos del predecessor actual
        if self.predecessor:
            pred_ip, pred_port, pred_id = self.predecessor
        else: # no hay predecessor
            pred_ip, pred_port, pred_id = None, None, None

        return {
            "type": "PREDECESSOR_RESPONSE",
            "predecessor_ip": pred_ip, #ip del predecessor
            "predecessor_port": pred_port, #puerto del predecessor
            "predecessor_id": pred_id, #id del predecessor
            "node_id": self.node_id, #id del nodo que responde
            "timestamp": time.time(),
        }


    """_handle_notify
    descripcion: Maneja notificación de posible nuevo predecessor
    entrada: message Diccionario con el mensaje CHORD_NOTIFY
    salida: None (no requiere respuesta)
    """
    def _handle_notify(self, message:  Dict) -> Optional[Dict]:

        #Obtenemos información necesaria
        new_node_id = message.get("node_id")
        new_ip = message.get("ip")
        new_port = message.get("port")
        
        #En caso de que hayan errores
        if not new_node_id or not new_ip or not new_port:
            logger.warning("NOTIFY con datos incompletos")
            return None
        
        logger.info(f"NOTIFY recibido de {new_node_id[: 8]}... ({new_ip}:{new_port})")
        
        # Caso 1: No hay predecessor
        if not self.predecessor:
            self.predecessor = (new_ip, new_port, new_node_id)
            logger.info(f"Predecessor establecido: {new_node_id[:8]}...")
            return None
        
        # Caso 2: El nodo que notifica es el mismo que mi predecessor actual
        if new_node_id == self.predecessor[2]:

            # Actualizar por si cambió IP/puerto (Es raro de que pase, pero por si acaso)
            self.predecessor = (new_ip, new_port, new_node_id)
            logger.debug(f"Predecessor confirmado: {new_node_id[:8]}...")
            return None
        
        # Caso 3: Verificar si el nuevo nodo está entre mi predecessor y yo
        if self._is_between(new_node_id, self. predecessor[2], self.node_id, inclusive=False):
            old_pred = self.predecessor
            self. predecessor = (new_ip, new_port, new_node_id)
            logger.info(f"Predecessor actualizado: {old_pred[2][:8]}... → {new_node_id[:8]}...")
        else:
            logger.debug(f"NOTIFY ignorado: {new_node_id[:8]}... no está entre predecessor y yo")
        
        return None 


    """_handle_predecessor_failure
    descripcion: Maneja la falla del predecessor.
    entrada: -
    salida: -"""
    def _send_heartbeat(self, target_ip: str, target_port: int) -> bool:
        # usar request_callback si está disponible
        if self.request_callback:
            logger.debug("Usando request_callback para heartbeat (envío en _wait_for_heartbeat_ack)")
            return True
        if not self.send_callback:
            return False
        
        message = {
            "type": "HEARTBEAT",
            "node_id": self.node_id, #id del nodo que envía el heartbeat
            "timestamp": time.time() 
        }
        
        try:
            # enviar heartbeat
            self.send_callback(target_ip, target_port, message)
            return True
        except Exception as e:
            logger.error(f"Error enviando heartbeat: {e}")
            return False


    """ _wait_for_heartbeat_ack
    descripcion: Espera respuesta de heartbeat (simplificado).
    entrada: target_id ID del nodo al que se envió el heartbeat, timeout tiempo máximo de espera
    salida: booleano indicando si se recibió respuesta"""
    def _wait_for_heartbeat_ack(self, target_id: str, timeout: int = 5) -> bool:
        """Espera respuesta de heartbeat usando request/response si está disponible."""
        # si no hay request_callback, no se puede esperar ACK
        if not self.request_callback:
            return True

        # enviar heartbeat tipo CHORD_ para que el receptor lo procese en overlay
        message = {
            "type": "CHORD_HEARTBEAT",
            "node_id": self.node_id,
            "timestamp": time.time(),
        }
        try:
            # resolver IP/puerto del target_id
            if self.predecessor and self.predecessor[2] == target_id: #si es el predecessor
                ip, port = self.predecessor[0], self.predecessor[1] #puerto del predecessor
            elif self.successor and self.successor[2] == target_id: #si es el successor
                ip, port = self.successor[0], self.successor[1] #puerto del successor
            elif target_id in self.neighbors:
                ip, port = self.neighbors[target_id]
            else:
                # sin datos del destino, no podemos pedir ACK contra ID.
                logger.debug("No se pudo resolver IP/puerto para heartbeat ACK")
                return False

            # enviar y esperar respuesta
            response = self.request_callback(ip, port, message)
            return bool(response and response.get("type") == "HEARTBEAT_ACK")
        except Exception as e:
            logger.error(f"Error esperando HEARTBEAT_ACK de {target_id[:8]}...: {e}")
            return False


    """_handle_predecessor_failure
        descripcion: Maneja la detección de fallo del predecesor.
        entrada: -
        salida: -"""
    def _handle_predecessor_failure(self):
        old_pred = self.predecessor
        if old_pred:
            logger.warning(f"Predecessor {old_pred[2][:8]}... detectado como caído")
        else:
            logger.warning("Predecessor ya era None al detectar fallo")

        # limpiar predecessor
        self.predecessor = None

        # intentar recuperación consultando al successor por su predecessor
        try:
            if self.successor:
                succ_ip, succ_port, _succ_id = self.successor
                recovered = self._ask_predecessor_of_successor(succ_ip, succ_port)
                if recovered and recovered[2] != self.node_id:
                    self.predecessor = recovered
                    logger.info(f"Predecesor recuperado: {recovered[2][:8]}...")
        except Exception as e:
            logger.error(f"Error recuperando predecessor tras fallo: {e}")

        # notificar al successor y actualizar finger table
        try:
            if self.successor:
                self._notify_successor(self.successor[0], self.successor[1])
        except Exception as e:
            logger.error(f"Error notificando al successor tras fallo de predecessor: {e}")

        try:
            self._update_finger_table()
        except Exception as e:
            logger.error(f"Error actualizando finger table tras fallo de predecessor: {e}")

        logger.info("Predecessor eliminado por fallo")