import json 
import time
from enum import Enum

#Definimos las categorias de los mensajes requeridos
class MessageType(Enum):
    JOIN = "JOIN"               #Para unirse a la red
    UPDATE = "UPDATE"           #Para actualizar informacion
    PUT = "PUT"                 #Guardar datos
    GET = "GET"                 #Obtiene una respuesta
    RESULT = "RESULT"           #Respuesta a una solicitud
    HEARTBEAT = "HEARTBEAT"     #Señal de vida
    ACK = "ACK"                 #Confirmación (réplicas)
    REPLICATE = "REPLICATE"     #Replicación de datos
    LOOKUP = "LOOKUP"           #Búsqueda distribuida Chord
    ERROR = "ERROR"             #Errores

#Mensaje dentro de la red P2P
class Message:
    def __init__(self, msg_type, sender_id, data=None):
        #Validación de los elementos
        if not isinstance(msg_type, MessageType) and msg_type not in MessageType.__members__:
            raise ValueError(f"Tipo de mensaje inválido: {msg_type}")
        
        self.type = msg_type                #Tipo de mensaje
        self.sender_id = sender_id          #ID del remitente
        self.data = data if data else {}    #Datos adicionales
        self.timestamp = time.time()        #Marca de tiempo del mensaje

    def to_dict(self):
        return{
            "type": self.type.value,
            "sender_id": self.sender_id,
            "data": self.data,
            "timestamp": self.timestamp
        }

def serializeMessage(message):
    if not isinstance(message, Message):
        raise ValueError("El objeto a serializar debe ser instancia de Message")
    return json.dumps(message.to_dict())

def deserialize_message(json_str: str) -> Message:
    try:
        dict_msg = json.loads(json_str)
        if "type" not in dict_msg or "sender_id" not in dict_msg:
            raise ValueError("Mensaje mal formado: faltan campos obligatorios (type, sender_id)")
            
        msg_type = MessageType(dict_msg["type"])
        sender_id = dict_msg["sender_id"]
        data = dict_msg.get("data", {})
        
        msg_obj = Message(msg_type, sender_id, data)
        if "timestamp" in dict_msg:
            msg_obj.timestamp = dict_msg["timestamp"]
        return msg_obj
    except json.JSONDecodeError:
        raise ValueError("Error al decodificar JSON: formato inválido")
    except ValueError as e:
        raise ValueError(f"Error de validación: {str(e)}")
