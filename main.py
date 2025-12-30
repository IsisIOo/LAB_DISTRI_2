"""
MAIN.PY COMPLETO - Con menú interactivo + Módulo 4 Storage
SIN logs spam + NOMBRES ÚNICOS + GET/PUT funcionando
"""
import time
from src.networking import TCPServer
from src.overlay import ChordNode
from src.protocol import Message, MessageType
from src.storage import DistributedStorage

# Variables globales
chord = None
pepe = None
storage = None
mi_ip = None
mi_puerto = None

def handle_incoming_message(msg: dict, addr: tuple):
    """
    Callback que se ejecuta cuando llega un mensaje al servidor.
    """
    msg_type = msg.get("type", "")
    
    # MENSAJES CHORD (internos)
    if msg_type.startswith("CHORD_") or msg_type in [
        "CHORD_GET_PRED", "CHORD_NOTIFY", "CHORD_GET_PREDECESSOR",
        "JOIN_REQUEST", "FIND_SUCCESSOR"
    ]:
        response = chord.handle_message(msg)
        if response:
            pepe.send_message(addr[0], addr[1], response)
        return
    
    # STORAGE MESSAGES (PUT/GET/REPLICATE/RESULT)
    if msg_type in ["PUT", "REPLICATE", "RESULT", "GET"]:
        response = storage.handle_storage_message(msg)
        if response:
            # Responder al ORIGEN (sender_ip/port) o addr
            sender_ip = msg.get("sender_ip", addr[0])
            sender_port = msg.get("sender_port", addr[1])
            pepe.send_message(sender_ip, sender_port, response)
        return
    
    # MENSAJES DE APLICACIÓN
    print(f"\n{'='*60}")
    print(f"MENSAJE RECIBIDO [{msg_type}]")
    print(f"{'='*60}")
    print(f"De: {addr[0]}:{addr[1]}")
    print(f"Tipo: {msg_type}")
    print(f"Remitente: {msg.get('sender_id', 'desconocido')}")
    print(f"Datos: {msg.get('data', {})}")
    print(f"{'='*60}\n")
    print(">>> ", end="", flush=True)
    
    # JOIN aplicación
    if msg_type == "JOIN":
        handle_join_app(msg, addr)
    return None

def handle_join_app(msg, addr):
    """Maneja mensaje JOIN de aplicación"""
    sender_id = msg.get('sender_id')
    data = msg.get('data', {})
    nombre = data.get('nombre', 'desconocido')
    
    print(f"🎉 {sender_id} [{nombre}] se unió a la red")
    
    response = Message(
        msg_type=MessageType.UPDATE,
        sender_id=chord.node_id[:8],
        data={
            "mensaje": "¡Bienvenido a la red Chord!",
            "mi_id": chord.node_id[:8],
            "successor": chord.successor
        }
    )
    pepe.send_message(addr[0], addr[1], response.to_dict())

def mostrar_menu():
    """Muestra el menú de comandos"""
    print(f"\n{'='*60}")
    print("COMANDOS DISPONIBLES - CHORD + STORAGE")
    print(f"{'='*60}")
    print("  join <ip> <puerto>           - Unir al anillo Chord")
    print("  put <clave> <valor>          - PUT distribuido (usa Chord)")
    print("  get <clave>                  - GET distribuido (usa Chord)")
    print("  storage                      - Ver storage local")
    print("  status                       - Estado Chord")
    print("  maintenance [on/off]         - Control spam")
    print("  help                         - Este menú")
    print("  quit                         - Salir")
    print(f"{'='*60}\n")

def main():
    global chord, pepe, storage, mi_ip, mi_puerto
    
    print("""
╔══════════════════════════════════════════════════════════╗
║     🚀 CHORD DHT COMPLETO - 4 MÓDULOS INTEGRADOS        ║
║  Networking + Protocol + Overlay + Distributed Storage  ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    # Configuración
    print("Configuración del nodo:")
    mi_ip = input("Tu IP (0.0.0.0 para todas): ").strip() or "0.0.0.0"
    mi_puerto_str = input("Tu puerto (5000): ").strip() or "5000"
    mi_puerto = int(mi_puerto_str)
    
    # ⭐ NOMBRE ÚNICO
    ultimo_octeto = mi_ip.split('.')[-1] if mi_ip != "0.0.0.0" else "0"
    nombre_nodo = f"Nodo-{ultimo_octeto}-{mi_puerto_str}"
    
    # Módulos
    pepe = TCPServer(mi_ip, mi_puerto, handle_incoming_message)
    pepe.start()
    print(f"📡 TCP {mi_ip}:{mi_puerto} [{nombre_nodo}]")
    
    chord = ChordNode(mi_ip, mi_puerto)
    chord.mi_ip = mi_ip
    chord.mi_puerto = mi_puerto
    chord.set_send_callback(pepe.send_message)
    
    storage = DistributedStorage(chord.node_id, pepe.send_message, chord)
    chord.maintenance_paused = True  # SIN SPAM
    print(f"✅ ID: {chord.node_id[:8]}  [PAUSADO]  R={storage.replication_factor}")
    
    # JOIN Chord
    join = input("\n¿Unirse a anillo existente? (s/n): ").strip().lower()
    if join == 's': 
        ip_bootstrap = input("IP bootstrap: ").strip()
        port_bootstrap = int(input("Puerto bootstrap: ").strip() or "5000")
        print(f"🔗 Uniéndose a {ip_bootstrap}:{port_bootstrap}...")
        chord.join_network((ip_bootstrap, port_bootstrap))
    else:
        print("🌟 Primer nodo del anillo")
        chord.is_joined = True
        chord.successor = (chord.ip, chord.port, chord.node_id)
        chord.start_maintenance()
    
    # Estado inicial
    print(f"\n{'='*60}")
    print(f"ESTADO NODO [{nombre_nodo}]")
    print(f"{'='*60}")
    info = chord.get_node_info()
    print(f"ID: {info['node_id'][:16]}...")
    print(f"IP: {mi_ip}:{mi_puerto}")
    print(f"Succ: {info['successor']}")
    print(f"Pred: {info['predecessor']}")
    print(f"Joined: {info['is_joined']}")
    print(f"Storage R={storage.replication_factor}")
    print(f"{'='*60}")
    
    mostrar_menu()
    
    # Loop interactivo
    try:
        while True:
            cmd = input(">>> ").strip().split()
            if not cmd: continue
            
            comando = cmd[0].lower()
            
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
                        "nombre": nombre_nodo
                    }
                )
                print(f"📤 JOIN [{nombre_nodo}] → {ip_destino}:{puerto_destino}")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            # ==================== PUT ====================
            elif comando == "put" and len(cmd) >= 3:
                key = cmd[1]
                value = " ".join(cmd[2:])
                responsible = chord.get_responsible_node(key)
                
                if responsible:
                    print(f"📤 PUT {key} → {responsible[2][:8]} ({responsible[0]}:{responsible[1]})")
                    mensaje = Message(
                        msg_type=MessageType.PUT,
                        sender_id=chord.node_id[:8],
                        data={"key": key, "value": value}
                    )
                    pepe.send_message(responsible[0], responsible[1], mensaje.to_dict())
                else:
                    print("❌ No hay nodo responsable")
            
            # ==================== GET ====================
            elif comando == "get" and len(cmd) >= 2:
                key = cmd[1]
                
                # LOCAL primero
                local_result = storage.get_local(key)
                if local_result:
                    print(f"✅ {key} = '{local_result['value']}' [LOCAL]")
                    continue
                
                responsible = chord.get_responsible_node(key)
                if responsible and not (responsible[0] == mi_ip and responsible[1] == mi_puerto):
                    print(f"🔍 GET {key} → {responsible[2][:8]} ({responsible[0]}:{responsible[1]})")
                    mensaje = Message(
                        msg_type=MessageType.GET,
                        sender_id=chord.node_id[:8],
                        data={"key": key}
                    )
                    pepe.send_message(responsible[0], responsible[1], mensaje.to_dict())
                    
                    time.sleep(3)  # Esperar respuesta
                    
                    result = storage.get_local(key)
                    if result:
                        print(f"✅ {key} = '{result['value']}' [REMOTO]")
                    else:
                        print(f"❌ {key} NO recibido")
                else:
                    print("❌ No hay nodo responsable")
            
            # ==================== STORAGE ====================
            elif comando == "storage":
                if storage.local_storage:
                    print(f"\n{'='*40}")
                    print(f"STORAGE LOCAL ({len(storage.local_storage)} claves)")
                    print(f"{'='*40}")
                    for k, v in storage.local_storage.items():
                        tipo = "R" if v.get("is_replica") else "P"
                        print(f"  {k}={v['value']} [{tipo}]")
                    print(f"{'='*40}\n")
                else:
                    print("Storage vacío\n")
            
            # ==================== STATUS ====================
            elif comando == "status":
                info = chord.get_node_info()
                print(f"\n{'='*60}")
                print(f"ESTADO [{nombre_nodo}]")
                print(f"{'='*60}")
                print(f"ID: {info['node_id'][:16]}...")
                print(f"IP: {mi_ip}:{mi_puerto}")
                print(f"Succ: {info['successor']}")
                print(f"Pred: {info['predecessor']}")
                print(f"Joined: {info['is_joined']}")
                print(f"Storage: {len(storage.local_storage)} claves")
                print(f"{'='*60}\n")
            
            # ==================== MAINTENANCE ====================
            elif comando == "maintenance":
                if len(cmd) > 1 and cmd[1] == "on":
                    chord.maintenance_paused = False
                    print("🔄 Mantenimiento ACTIVADO")
                else:
                    chord.maintenance_paused = True
                    print("⏸️ Mantenimiento PAUSADO (SIN SPAM)")
            
            elif comando == "help":
                mostrar_menu()
            
            elif comando == "quit":
                print("\n👋 Cerrando nodo...")
                break
            
            else:
                print("❓ put/get/storage/status/join/maintenance/quit")
    
    except KeyboardInterrupt:
        print("\n\nCtrl+C detectado...")
    
    finally:
        if chord: 
            chord.leave_network()
        if pepe: 
            pepe.stop()
        print("✅ Nodo cerrado correctamente")

if __name__ == "__main__":
    main()
