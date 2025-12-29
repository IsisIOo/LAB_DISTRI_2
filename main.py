"""
MAIN.PY COMPLETO - Integrado con Módulo 4 Storage
"""
import time
from src.networking import TCPServer
from src.overlay import ChordNode
from src.protocol import Message, MessageType
from src.storage import DistributedStorage  # ← NUEVO


# Variables globales
chord = None
pepe = None
storage = None  # ← REEMPLAZA local_storage


def handle_incoming_message(msg: dict, addr: tuple):
    msg_type = msg.get("type", "")
    
    # ⭐ CHORD MENSJES (PRIORIDAD 1)
    if msg_type.startswith("CHORD_") or msg_type in [
        "CHORD_GET_PRED", "CHORD_NOTIFY", "CHORD_GET_PREDECESSOR",
        "JOIN_REQUEST", "FIND_SUCCESSOR"
    ]:
        response = chord.handle_message(msg)
        if response:
            pepe.send_message(addr[0], addr[1], response)
        return
    
    # ⭐ STORAGE (PUT/GET/REPLICATE) - ROUTING DISTRIBUIDO
    elif msg_type in ["PUT", "GET", "REPLICATE"]:
        handle_storage_distributed(msg, addr)
        return
    
    # JOIN aplicación
    elif msg_type == "JOIN":
        handle_join_app(msg, addr)
    elif msg_type == "HEARTBEAT":
        handle_heartbeat(msg, addr)

def handle_storage_distributed(msg: dict, addr: tuple):
    """Routing distribuido para PUT/GET/REPLICATE"""
    data = msg.get('data', {})
    key = data.get('key')
    
    if not key:
        return
    
    msg_type = msg.get("type")
    responsible = chord.get_responsible_node(key)
    
    if responsible and responsible[2] == chord.node_id:
        # YO soy responsable
        print(f"💾🔍 [{chord.node_id[:8]}] {msg_type} LOCAL: {key}")
        response = storage.handle_storage_message(msg)
        pepe.send_message(addr[0], addr[1], response)
    else:
        # Reenviar al responsable
        if responsible:
            print(f"↩️ [{chord.node_id[:8]}] {msg_type} {key} → {responsible[2][:8]}")
            pepe.send_message(responsible[0], responsible[1], msg)



def handle_join_app(msg, addr):
    """Maneja mensaje JOIN de aplicación"""
    sender_id = msg.get('sender_id')
    print(f"🤝 {sender_id} se quiere unir a la red")
    
    response = Message(
        msg_type=MessageType.UPDATE,
        sender_id=chord.node_id[:8],
        data={
            "mensaje": "¡Bienvenido a la red!",
            "mi_id": chord.node_id[:8],
            "nodos_en_anillo": "Al menos 2 (tú y yo)"
        }
    )
    pepe.send_message(addr[0], addr[1], response.to_dict())


def handle_heartbeat(msg, addr):
    """Maneja HEARTBEAT"""
    sender_id = msg.get('sender_id')
    print(f"💓 HEARTBEAT de {sender_id}")


def mostrar_menu():
    """Muestra el menú de comandos"""
    print(f"\n{'='*60}")
    print("COMANDOS DISPONIBLES")
    print(f"{'='*60}")
    print("  join <ip> <puerto>           - Enviar JOIN a otro nodo")
    print("  put <clave> <valor>          - Guardar datos (usa Chord + Storage)")
    print("  get <clave>                  - Buscar datos (usa Chord + Storage)")
    print("  send <ip> <puerto> <mensaje> - Enviar mensaje libre")
    print("  heartbeat <ip> <puerto>      - Enviar heartbeat")
    print("  storage                      - Ver datos almacenados localmente")
    print("  storage stats                - Estadísticas del storage")
    print("  status                       - Ver estado del nodo Chord")
    print("  help                         - Mostrar este menú")
    print("  quit                         - Salir")
    print(f"{'='*60}\n")


def main():
    global chord, pepe, storage
    
    print("""
╔══════════════════════════════════════════════════════════╗
║       LABORATORIO DISTRIBUIDO - SISTEMA P2P CHORD        ║
║    Networking + Protocol + Overlay + STORAGE Integrados  ║
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
    
    # 🆕 CREAR STORAGE (Módulo 4) - INTEGRACIÓN COMPLETA
    storage = DistributedStorage(chord.node_id, pepe.send_message)
    
    print("\nEsperando que el servidor inicie...")
    time.sleep(2)
    
    # Menú para unirse al anillo Chord
    join = input("\n¿Unirse a un anillo Chord existente? (s/n): ").strip().lower()
    
    if join == 's': 
        ip_bootstrap = input("IP del nodo bootstrap: ").strip()
        port_bootstrap = input("Puerto del nodo bootstrap: ").strip()
        port_bootstrap = int(port_bootstrap) if port_bootstrap else 5000
        
        print(f"\n⏳ Uniéndose al anillo Chord vía {ip_bootstrap}:{port_bootstrap}...")
        chord.join_network((ip_bootstrap, port_bootstrap))
        time.sleep(2)
    else:
        print("\n✅ Este es el primer nodo del anillo Chord")
        chord.is_joined = True
        chord.successor = (mi_ip, mi_puerto, chord.node_id)
    
    # Mostrar estado inicial COMPLETO
    print(f"\n{'='*60}")
    print("ESTADO INICIAL DEL NODO:")
    info = chord.get_node_info()
    print(f"ID Chord: {info['node_id'][:16]}...")
    print(f"IP:Puerto: {mi_ip}:{mi_puerto}")
    print(f"Successor: {info['successor']}")
    print(f"Predecessor: {info['predecessor']}")
    print(f"En anillo: {info['is_joined']}")
    print(f"Storage listo: R={storage.replication_factor}")
    print(f"{'='*60}")
    
    mostrar_menu()
    
    # Loop interactivo
    try:
        while True:
            cmd = input(">>> ").strip().split()
            if not cmd:
                continue
            
            comando = cmd[0].lower()
            
            # ==================== JOIN ====================
            if comando == "join" and len(cmd) >= 3:
                ip_destino = cmd[1]
                puerto_destino = int(cmd[2])
                
                mensaje = Message(
                    msg_type=MessageType.JOIN,
                    sender_id=chord.node_id[:8],
                    data={"ip": mi_ip, "port": mi_puerto, "nombre": f"Nodo_{mi_puerto}"}
                )
                print(f"📤 Enviando JOIN a {ip_destino}:{puerto_destino}...")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            # ==================== PUT ==================== (USANDO STORAGE)
            elif comando == "put" and len(cmd) >= 3:
                key = cmd[1]
                value = " ".join(cmd[2:])
                
                # 1. Chord encuentra responsable
                responsible = chord.get_responsible_node(key)
                
                if responsible:
                    print(f"→ Nodo responsable: {responsible[2][:8]} ({responsible[0]}:{responsible[1]})")
                    
                    # 2. Storage crea mensaje PUT
                    put_result = storage.put(key, value)
                    mensaje = put_result["message"]
                    
                    # 3. Networking envía
                    pepe.send_message(responsible[0], responsible[1], mensaje)
                    print(f"📤 PUT {key} enviado")
                else:
                    print("❌ No se pudo determinar nodo responsable")
            
            # ==================== GET ==================== (USANDO STORAGE)
            elif comando == "get" and len(cmd) >= 2:
                key = cmd[1]
                
                responsible = chord.get_responsible_node(key)
                if responsible:
                    print(f"→ Preguntando a: {responsible[2][:8]} ({responsible[0]}:{responsible[1]})")
                    
                    # Storage crea mensaje GET
                    get_result = storage.get(key)
                    mensaje = get_result["message"]
                    
                    pepe.send_message(responsible[0], responsible[1], mensaje)
                    print(f"🔍 GET {key} enviado")
                else:
                    print("❌ No se pudo determinar nodo responsable")
            
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
                print(f"📤 Enviando a {ip_destino}:{puerto_destino}: {mensaje_texto}")
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
                print(f"💓 Enviando HEARTBEAT a {ip_destino}:{puerto_destino}")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            # ==================== STORAGE ====================
            elif comando == "storage":
                if storage.local_storage:
                    print(f"\n{'='*60}")
                    print(f"ALMACENAMIENTO LOCAL ({len(storage.local_storage)} claves)")
                    print(f"{'='*60}")
                    for k, v in storage.local_storage.items():
                        tipo = "[REPLICA]" if v.get("is_replica") else "[PRIMARY]"
                        print(f"  {k} = {v['value']} {tipo} (hash={v['key_hash'][:8]})")
                    print(f"{'='*60}\n")
                else:
                    print("\n⚠️ No hay datos almacenados localmente\n")
            
            elif comando == "storage" and len(cmd) > 1 and cmd[1] == "stats":
                stats = storage.get_stats()
                print(f"\n📊 ESTADÍSTICAS STORAGE:")
                print(f"  Total claves: {stats['total_keys']}")
                print(f"  Primarias: {stats['primaries']}")
                print(f"  Réplicas: {stats['replicas']}")
                print(f"  Factor R: {stats['replication_factor']}\n")
            
            # ==================== STATUS ====================
            elif comando == "status":
                info = chord.get_node_info()
                print(f"\n{'='*60}")
                print("ESTADO DEL NODO COMPLETO")
                print(f"{'='*60}")
                print(f"ID: {info['node_id'][:16]}...")
                print(f"IP:Puerto: {mi_ip}:{mi_puerto}")
                print(f"Successor: {info['successor']}")
                print(f"Predecessor: {info['predecessor']}")
                print(f"En anillo: {info['is_joined']}")
                print(f"Datos: {storage.get_stats()['total_keys']} claves")
                print(f"{'='*60}\n")
            
            elif comando == "stabilize":
                chord.stabilize_now()
                print("🔄 Estabilización ejecutada")
                print(chord.get_node_info())
            
            elif comando == "neighbors":
                print(f"Vecinos conocidos ({len(chord.neighbors)}):")
                for nid, (ip, port) in chord.neighbors.items():
                    print(f"  {nid[:8]} → {ip}:{port}")

            elif comando == "help":
                mostrar_menu()
            
            elif comando == "quit":
                print("\n👋 Cerrando nodo...")
                break
            
            else:
                print("❌ Comando no reconocido. Usa 'help'")
    
    except KeyboardInterrupt:
        print("\n\nCtrl+C detectado...")
    
    finally:
        if chord:
            chord.leave_network()
        if pepe:
            pepe.stop()
        print("\n✅ Nodo cerrado. ¡Hasta luego!")


if __name__ == "__main__":
    main()
