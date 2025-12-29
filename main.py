"""
MAIN.PY COMPLETO - Con menú interactivo
"""
import time
from src.networking import TCPServer
from src.overlay import ChordNode
from src.protocol import Message, MessageType

# Variables globales
chord = None
pepe = None
local_storage = {}  # Almacenamiento local simple

def handle_incoming_message(msg:  dict, addr: tuple):
    """
    Callback que se ejecuta cuando llega un mensaje al servidor.
    Si retorna un dict, el servidor lo enviará por el mismo socket.
    """
    msg_type = msg. get("type", "")
    
    # MENSAJES CHORD (internos)
    if msg_type. startswith("CHORD_"):
        # Responder directamente por el mismo socket (networking maneja el envío)
        return chord.handle_message(msg)
    
    # MENSAJES DE APLICACIÓN (los que TÚ envías)
    else:
        print(f"\n{'='*60}")
        print(f"MENSAJE RECIBIDO")
        print(f"{'='*60}")
        print(f"De: {addr[0]}:{addr[1]}")
        print(f"Tipo: {msg_type}")
        print(f"Remitente: {msg.get('sender_id', 'desconocido')}")
        print(f"Datos: {msg.get('data', {})}")
        print(f"{'='*60}\n")
        print(">>> ", end="", flush=True)  # Volver a mostrar prompt
        
        # Procesar según tipo
        if msg_type == "JOIN":
            handle_join_app(msg, addr)
        elif msg_type == "PUT": 
            handle_put(msg, addr)
        elif msg_type == "GET":
            handle_get(msg, addr)
        elif msg_type == "HEARTBEAT":
            handle_heartbeat(msg, addr)
        # No retornamos respuesta para mensajes de aplicación en este flujo
        return None

def handle_join_app(msg, addr):
    """Maneja mensaje JOIN de aplicación"""
    sender_id = msg.get('sender_id')
    data = msg.get('data', {})
    
    print(f"{sender_id} se quiere unir a la red")
    
    # Responder con bienvenida
    response = Message(
        msg_type=MessageType.UPDATE,
        sender_id=chord.node_id[: 8],
        data={
            "mensaje": "¡Bienvenido a la red!",
            "mi_id": chord.node_id[:8],
            "nodos_en_anillo": "Al menos 2 (tú y yo)"
        }
    )
    pepe.send_message(addr[0], addr[1], response. to_dict())

def handle_put(msg, addr):
    """Maneja operación PUT"""
    data = msg.get('data', {})
    key = data.get('key')
    value = data.get('value')
    
    if not key: 
        return
    
    # Determinar quién es responsable según Chord
    responsible = chord.get_responsible_node(key)
    
    if responsible and responsible[2] == chord.node_id:
        # YO soy responsable:  guardar localmente
        local_storage[key] = value
        print(f"Guardado:  {key} = {value}")
        
        # Responder con confirmación
        response = Message(
            msg_type=MessageType. RESULT,
            sender_id=chord.node_id[:8],
            data={
                "key": key,
                "status": "stored",
                "node":  chord.node_id[: 8]
            }
        )
        pepe.send_message(addr[0], addr[1], response.to_dict())
    else:
        # NO soy responsable: reenviar al nodo correcto
        if responsible:
            print(f"↪Reenviando PUT a nodo {responsible[2][: 8]}...")
            pepe.send_message(responsible[0], responsible[1], msg)

def handle_get(msg, addr):
    """Maneja operación GET"""
    data = msg.get('data', {})
    key = data.get('key')
    
    if not key:
        return
    
    # Determinar quién es responsable
    responsible = chord.get_responsible_node(key)
    
    if responsible and responsible[2] == chord.node_id:
        # YO soy responsable: buscar localmente
        value = local_storage.get(key)
        
        print(f"Buscando: {key} → {value if value else 'NO ENCONTRADO'}")
        
        response = Message(
            msg_type=MessageType.RESULT,
            sender_id=chord.node_id[:8],
            data={
                "key": key,
                "value": value,
                "found": value is not None,
                "node": chord.node_id[:8]
            }
        )
        pepe.send_message(addr[0], addr[1], response.to_dict())
    else:
        # NO soy responsable: reenviar
        if responsible:
            print(f"↪️ Reenviando GET a nodo {responsible[2][:8]}...")
            pepe.send_message(responsible[0], responsible[1], msg)

def handle_heartbeat(msg, addr):
    """Maneja HEARTBEAT"""
    sender_id = msg.get('sender_id')
    print(f"HEARTBEAT de {sender_id}")

def mostrar_menu():
    """Muestra el menú de comandos"""
    print(f"\n{'='*60}")
    print("COMANDOS DISPONIBLES")
    print(f"{'='*60}")
    print("  join <ip> <puerto>           - Enviar JOIN a otro nodo")
    print("  put <clave> <valor>          - Guardar datos (usa Chord)")
    print("  get <clave>                  - Buscar datos (usa Chord)")
    print("  send <ip> <puerto> <mensaje> - Enviar mensaje libre")
    print("  heartbeat <ip> <puerto>      - Enviar heartbeat")
    print("  storage                      - Ver datos almacenados localmente")
    print("  status                       - Ver estado del nodo Chord")
    print("  help                         - Mostrar este menú")
    print("  quit                         - Salir")
    print(f"{'='*60}\n")

def main():
    global chord, pepe, local_storage
    
    print("""
╔══════════════════════════════════════════════════════════╗
║       LABORATORIO DISTRIBUIDO - SISTEMA P2P CHORD        ║
║         Networking + Protocol + Overlay Integrados       ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    # Configuración
    print("Configuración del nodo:")
    mi_ip = input("Tu IP (0.0.0.0 para escuchar en todas): ").strip() or "0.0.0.0"
    mi_puerto = input("Tu puerto (default 5000): ").strip()
    mi_puerto = int(mi_puerto) if mi_puerto else 5000
    
    # Crear servidor TCP (Módulo 1)
    pepe = TCPServer(mi_ip, mi_puerto, handle_incoming_message)
    pepe.start()
    
    # Crear nodo Chord (Módulo 3)
    chord = ChordNode(mi_ip, mi_puerto)
    chord.set_send_callback(pepe.send_message)
    # Callback síncrono para peticiones de CHORD que esperan respuesta inmediata
    chord.set_request_callback(pepe.request_response)
    
    print("\nEsperando que el servidor inicie...")
    time.sleep(2)
    
    # Menú para unirse al anillo Chord
    join = input("\n¿Unirse a un anillo Chord existente? (s/n): ").strip().lower()
    
    if join == 's': 
        ip_bootstrap = input("IP del nodo bootstrap:  ").strip()
        port_bootstrap = input("Puerto del nodo bootstrap: ").strip()
        port_bootstrap = int(port_bootstrap) if port_bootstrap else 5000
        
        print(f"\nUniéndose al anillo Chord vía {ip_bootstrap}:{port_bootstrap}...")
        chord.join_network((ip_bootstrap, port_bootstrap))
        time.sleep(2)
    else:
        print("\nEste es el primer nodo del anillo Chord")
        chord.is_joined = True
        chord.successor = (chord.ip, chord.port, chord.node_id)
        #print("Successor: None (primer nodo del anillo)")
        #print("Predecessor: None (anillo de 1 nodo)") 

        # Iniciar hilos de mantenimiento para que el anillo se estabilice dinámicamente
        chord.start_maintenance()
    
    # Mostrar estado inicial
    print(f"\n{'='*60}")
    print("ESTADO INICIAL DEL NODO:")
    info = chord.get_node_info()
    print(f"ID Chord: {info['node_id'][: 16]}...")
    print(f"IP: Puerto: {mi_ip}:{mi_puerto}")
    print(f"Successor: {info['successor']}")
    print(f"Predecessor: {info['predecessor']}")
    print(f"En anillo: {info['is_joined']}")
    print(f"{'='*60}")
    
    # Mostrar menú
    mostrar_menu()
    
    # Loop interactivo
    try:
        while True:
            cmd = input(">>> ").strip().split()
            
            if not cmd:
                continue
            
            comando = cmd[0]. lower()
            
            # ==================== JOIN ====================
            if comando == "join" and len(cmd) >= 3:
                ip_destino = cmd[1]
                puerto_destino = int(cmd[2])
                
                mensaje = Message(
                    msg_type=MessageType.JOIN,
                    sender_id=chord.node_id[:8],
                    data={
                        "ip": mi_ip,
                        "port": mi_puerto,
                        "nombre": f"Nodo_{mi_puerto}"
                    }
                )
                
                print(f"Enviando JOIN a {ip_destino}:{puerto_destino}...")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            # ==================== PUT ====================
            elif comando == "put" and len(cmd) >= 3:
                key = cmd[1]
                value = " ".join(cmd[2:])
                
                print(f"\nPUT: {key} = {value}")
                
                # Chord determina quién es responsable
                responsible = chord.get_responsible_node(key)
                
                if responsible: 
                    print(f"→ Nodo responsable: {responsible[2][:8]}...  ({responsible[0]}:{responsible[1]})")
                    
                    mensaje = Message(
                        msg_type=MessageType.PUT,
                        sender_id=chord. node_id[:8],
                        data={"key": key, "value":  value}
                    )
                    
                    pepe.send_message(responsible[0], responsible[1], mensaje. to_dict())
                else: 
                    print("No se pudo determinar nodo responsable")
            
            # ==================== GET ====================
            elif comando == "get" and len(cmd) >= 2:
                key = cmd[1]
                
                print(f"\nGET: {key}")
                
                # Chord determina quién es responsable
                responsible = chord.get_responsible_node(key)
                
                if responsible:
                    print(f"→ Preguntando a nodo:  {responsible[2][:8]}... ({responsible[0]}:{responsible[1]})")
                    
                    mensaje = Message(
                        msg_type=MessageType. GET,
                        sender_id=chord.node_id[:8],
                        data={"key": key}
                    )
                    
                    pepe. send_message(responsible[0], responsible[1], mensaje.to_dict())
                else:
                    print(" No se pudo determinar nodo responsable")
            
            # ==================== SEND ====================
            elif comando == "send" and len(cmd) >= 4:
                ip_destino = cmd[1]
                puerto_destino = int(cmd[2])
                mensaje_texto = " ".join(cmd[3:])
                
                mensaje = {
                    "type": "CUSTOM",
                    "sender_id": chord.node_id[:8],
                    "data": {"mensaje": mensaje_texto}
                }
                
                print(f"Enviando a {ip_destino}:{puerto_destino}:  {mensaje_texto}")
                pepe.send_message(ip_destino, puerto_destino, mensaje)
            
            # ==================== HEARTBEAT ====================
            elif comando == "heartbeat" and len(cmd) >= 3:
                ip_destino = cmd[1]
                puerto_destino = int(cmd[2])
                
                mensaje = Message(
                    msg_type=MessageType.HEARTBEAT,
                    sender_id=chord.node_id[:8],
                    data={"status": "alive"}
                )
                
                print(f"Enviando HEARTBEAT a {ip_destino}:{puerto_destino}")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            # ==================== STORAGE ====================
            elif comando == "storage": 
                if local_storage:
                    print(f"\n{'='*60}")
                    print(f"ALMACENAMIENTO LOCAL ({len(local_storage)} claves)")
                    print(f"{'='*60}")
                    for k, v in local_storage.items():
                        print(f"  {k} = {v}")
                    print(f"{'='*60}\n")
                else:
                    print("\n No hay datos almacenados localmente\n")
            
            # ==================== STATUS ====================
            elif comando == "status":
                info = chord.get_node_info()
                print(f"\n{'='*60}")
                print("ESTADO DEL NODO CHORD")
                print(f"{'='*60}")
                print(f"ID: {info['node_id'][:16]}...")
                print(f"IP:Puerto:  {mi_ip}:{mi_puerto}")
                print(f"Successor: {info['successor']}")
                print(f"Predecessor: {info['predecessor']}")
                print(f"En anillo: {info['is_joined']}")
                print(f"Datos almacenados: {len(local_storage)}")
                print(f"{'='*60}\n")
            
            # ==================== HELP ====================
            elif comando == "help":
                mostrar_menu()
            
            # ==================== QUIT ====================
            elif comando == "quit": 
                print("\nCerrando nodo...")
                break
            
            else:
                print("Comando no reconocido.  Usa 'help' para ver comandos")
    
    except KeyboardInterrupt:
        print("\n\nCtrl+C detectado...")
    
    finally:
        chord.leave_network()
        pepe.stop()
        print("\nNodo cerrado. ¡Hasta nunca!")

if __name__ == "__main__":
    main()