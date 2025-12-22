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

#Convierte el mensaje a un diccionario python
    def to_dict(self):
        return{
            "type": self.type.value,
            "sender_id": self.sender_id,
            "data": self.data,
            "timestamp": self.timestamp
        }

#Convierte el mensaje a formato JSON para enviarlo en la red
def serializeMessage(message):
    if not isinstance(message, Message):
        raise ValueError("El objeto a serializar debe ser instancia de Message")
    
    return json.dumps(message.to_dict())

def deserialize_message(json_str: str) -> Message:
    """
    Convierte un string JSON recibido desde la red a un objeto Message.
    Incluye validación de estructura.
    """
    try:
        # 1. Parsear el JSON
        dict_msg = json.loads(json_str)
        
        # 2. Validar campos obligatorios
        if "type" not in dict_msg or "sender_id" not in dict_msg:
            raise ValueError("Mensaje mal formado: faltan campos obligatorios (type, sender_id)")
            
        # 3. Reconstruir el objeto Message
        # Nota: Convertimos el string del tipo de vuelta al Enum MessageType
        msg_type = MessageType(dict_msg["type"])
        sender_id = dict_msg["sender_id"]
        data = dict_msg.get("data", {}) # Si no trae data, usamos vacío
        
        msg_obj = Message(msg_type, sender_id, data)
        
        # Restauramos el timestamp original si viene en el mensaje
        if "timestamp" in dict_msg:
            msg_obj.timestamp = dict_msg["timestamp"]
            
        return msg_obj

    except json.JSONDecodeError:
        raise ValueError("Error al decodificar JSON: formato inválido")
    except ValueError as e:
        raise ValueError(f"Error de validación: {str(e)}")