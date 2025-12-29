"""
MAIN.PY COMPLETO - Integrado con Módulo 4 Storage (SIN logs spam + NOMBRES ÚNICOS)
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

def handle_incoming_message(msg: dict, addr: tuple):
    msg_type = msg.get("type", "")
    
    # CHORD MENSJES
    if msg_type.startswith("CHORD_") or msg_type in [
        "CHORD_GET_PRED", "CHORD_NOTIFY", "CHORD_GET_PREDECESSOR",
        "JOIN_REQUEST", "FIND_SUCCESSOR"
    ]:
        response = chord.handle_message(msg)
        if response:
            pepe.send_message(addr[0], addr[1], response)
        return
    
    # STORAGE (PUT/GET/REPLICATE) - ROUTING SILENCIOSO
    elif msg_type in ["PUT", "GET", "REPLICATE", "RESULT"]:
        if msg_type == "RESULT":
            storage._handle_result(msg)  # ← GET respuesta
        else:
            handle_storage_distributed(msg, addr)
        return
    
    # JOIN aplicación
    elif msg_type == "JOIN":
        handle_join_app(msg, addr)

def handle_storage_distributed(msg: dict, addr: tuple):
    """Routing SILENCIOSO"""
    data = msg.get('data', {})
    key = data.get('key')
    
    if not key:
        return
    
    responsible = chord.get_responsible_node(key)
    
    if responsible and responsible[2] == chord.node_id:
        response = storage.handle_storage_message(msg)
        if response:
            pepe.send_message(addr[0], addr[1], response)
    elif responsible:
        pepe.send_message(responsible[0], responsible[1], msg)

def handle_join_app(msg, addr):
    """JOIN silencioso"""
    response = Message(
        msg_type=MessageType.UPDATE,
        sender_id=chord.node_id[:8],
        data={"successor": chord.successor}
    )
    pepe.send_message(addr[0], addr[1], response.to_dict())

def mostrar_menu():
    print(f"\n{'='*50}")
    print("COMANDOS")
    print("  put <key> <value>  get <key>  status  storage  quit")
    print(f"{'='*50}\n")

def main():
    global chord, pepe, storage
    
    print("🚀 P2P CHORD - 4 MÓDULOS INTEGRADOS")
    
    # Configuración
    mi_ip = input("IP (0.0.0.0): ").strip() or "0.0.0.0"
    mi_puerto_str = input("Puerto (5000): ").strip() or "5000"  # ⭐ GUARDAR STRING
    mi_puerto = int(mi_puerto_str)
    
    # ⭐ NOMBRE ÚNICO (último octeto IP + puerto HOST)
    ultimo_octeto = mi_ip.split('.')[-1] if mi_ip != "0.0.0.0" else "0"
    nombre_nodo = f"Nodo-{ultimo_octeto}-{mi_puerto_str}"
    
    # Módulos
    pepe = TCPServer(mi_ip, mi_puerto, handle_incoming_message)
    pepe.start()
    print(f"📡 TCP {mi_ip}:{mi_puerto} [{nombre_nodo}]")
    
    chord = ChordNode(mi_ip, mi_puerto)
    chord.set_send_callback(pepe.send_message)
    
    storage = DistributedStorage(chord.node_id, pepe.send_message, chord)

        # Después de crear chord:
    chord.maintenance_paused = True  # ⭐ SIN SPAM
    print(f"✅ ID: {chord.node_id[:8]}  [PAUSADO]  R={storage.replication_factor}")

        
    # JOIN
    join = input("¿Unirse? (s/n): ").strip().lower()
    if join == 's':
        ip_b = input("IP bootstrap: ").strip()
        port_b = int(input("Puerto bootstrap: ").strip() or "5000")
        chord.join_network((ip_b, port_b))
    else:
        chord.is_joined = True
    
    print(f"✅ ID: {chord.node_id[:8]}  [{nombre_nodo}]  R={storage.replication_factor}")
    mostrar_menu()
    
    # Loop comandos
    try:
        while True:
            cmd = input(">>> ").strip().split()
            if not cmd: continue
            
            comando = cmd[0].lower()
            
            # PUT
            if comando == "put" and len(cmd) >= 3:
                key, value = cmd[1], " ".join(cmd[2:])
                responsible = chord.get_responsible_node(key)
                if responsible:
                    storage.put(key, value)
                    print(f"📤 {key} → {responsible[2][:8]}")
            
            # GET SÍNCRONO
            elif comando == "get" and len(cmd) >= 2:
                key = cmd[1]
                result = storage.get(key)
                if result and result.get("data", {}).get("found"):
                    data = result["data"]
                    print(f"✅ {key} = '{data['value']}' [{data.get('node', 'local')}]")
                else:
                    print(f"❌ {key} no encontrado")
            
            # STATUS
            elif comando == "status":
                info = chord.get_node_info()
                print(f"ID: {info['node_id'][:8]} [{nombre_nodo}]")
                print(f"Succ: {info['successor']}")
                print(f"Pred: {info['predecessor']}")
                print(f"Joined: {info['is_joined']}")
            
            # STORAGE
            elif comando == "storage":
                if storage.local_storage:
                    for k, v in storage.local_storage.items():
                        tipo = "R" if v.get("is_replica") else "P"
                        print(f"  {k}={v['value']} [{tipo}]")
                else:
                    print("vacío")
            
            # JOIN MANUAL con nombre único
            elif comando == "join" and len(cmd) >= 3:
                ip_destino = cmd[1]
                puerto_destino = int(cmd[2])
                
                mensaje = Message(
                    msg_type=MessageType.JOIN,
                    sender_id=chord.node_id[:8],
                    data={
                        "ip": mi_ip, 
                        "port": mi_puerto, 
                        "nombre": nombre_nodo  # ⭐ NOMBRE ÚNICO!
                    }
                )
                print(f"📤 JOIN {nombre_nodo} → {ip_destino}:{puerto_destino}")
                pepe.send_message(ip_destino, puerto_destino, mensaje.to_dict())
            
            elif comando == "maintenance":
                if len(cmd) > 1 and cmd[1] == "on":
                    chord.maintenance_paused = False
                    print("🔄 Mantenimiento ACTIVADO")
                else:
                    chord.maintenance_paused = True
                    print("⏸️ Mantenimiento PAUSADO (sin mensajes spam)")

            elif comando == "stabilize":
                chord.maintenance_paused = False
                chord._stabilize()
                chord.maintenance_paused = True
                print("🔄 Stabilize una vez")

            elif comando == "quit":
                break
            
            else:
                print("put/get/status/storage/join/quit")
    
    finally:
        if chord: chord.leave_network()
        if pepe: pepe.stop()

if __name__ == "__main__":
    main()
