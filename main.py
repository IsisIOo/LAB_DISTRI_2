"""
MAIN SIMPLE - Para entender cómo funciona
Cada laptop ejecuta este programa
"""
import time
from src.networking import TCPServer
from src. protocol import Message, MessageType

def handle_incoming_message(msg:  dict, addr: tuple):
    """Se ejecuta cuando llega un mensaje"""
    print(f"\n{'='*60}")
    print(f"📨 MENSAJE RECIBIDO de {addr[0]}:{addr[1]}")
    print(f"{'='*60}")
    print(f"Tipo: {msg. get('type')}")
    print(f"De: {msg.get('sender_id')}")
    print(f"Datos: {msg.get('data')}")
    print(f"{'='*60}\n")

def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║           NODO P2P - LABORATORIO DISTRIBUIDO             ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    # CONFIGURACIÓN
    print("📝 Configuración de tu nodo:")
    mi_puerto = input("Puerto para escuchar (ej: 5000): ").strip()
    mi_puerto = int(mi_puerto) if mi_puerto else 5000
    
    # CREAR SERVIDOR (Módulo 1)
    servidor = TCPServer('0.0.0.0', mi_puerto, handle_incoming_message)
    servidor.start()
    
    print(f"\n✅ Tu nodo está escuchando en puerto {mi_puerto}")
    print(f"💡 Tu IP: [Averigua con ipconfig/ifconfig]")
    print(f"\n{'='*60}")
    
    # ESPERAR A QUE EL SERVIDOR INICIE
    time.sleep(2)
    
    # MENÚ INTERACTIVO
    print("\nCOMANDOS:")
    print("  enviar  - Enviar un mensaje a otro nodo")
    print("  test    - Enviar mensaje a ti mismo (prueba)")
    print("  quit    - Salir")
    print("="*60 + "\n")
    
    try:
        while True:
            comando = input(">>> ").strip().lower()
            
            if comando == "enviar":
                print("\n📤 Enviar mensaje a otro nodo:")
                ip_destino = input("  IP del nodo destino: ").strip()
                puerto_destino = input("  Puerto del nodo destino: ").strip()
                puerto_destino = int(puerto_destino) if puerto_destino else 5000
                
                # SELECCIONAR TIPO DE MENSAJE
                print("\n  Tipo de mensaje:")
                print("    1. JOIN")
                print("    2. PUT")
                print("    3. GET")
                print("    4. HEARTBEAT")
                tipo_opcion = input("  Selecciona (1-4): ").strip()
                
                # CREAR MENSAJE USANDO EL MÓDULO 2 (Protocol) ✅
                if tipo_opcion == "1":
                    mensaje = Message(
                        msg_type=MessageType.JOIN,
                        sender_id=f"nodo_puerto_{mi_puerto}",
                        data={
                            "ip": "0.0.0.0",  # Tu IP
                            "port": mi_puerto,
                            "mensaje": "Hola, quiero unirme!"
                        }
                    )
                
                elif tipo_opcion == "2": 
                    clave = input("  Clave:  ").strip()
                    valor = input("  Valor: ").strip()
                    mensaje = Message(
                        msg_type=MessageType.PUT,
                        sender_id=f"nodo_puerto_{mi_puerto}",
                        data={"key": clave, "value": valor}
                    )
                
                elif tipo_opcion == "3":
                    clave = input("  Clave a buscar: ").strip()
                    mensaje = Message(
                        msg_type=MessageType. GET,
                        sender_id=f"nodo_puerto_{mi_puerto}",
                        data={"key": clave}
                    )
                
                else:
                    mensaje = Message(
                        msg_type=MessageType.HEARTBEAT,
                        sender_id=f"nodo_puerto_{mi_puerto}",
                        data={"status": "alive"}
                    )
                
                # ENVIAR usando Módulo 1 (Networking) ✅
                print(f"\n📤 Enviando {mensaje.type.value} a {ip_destino}:{puerto_destino}...")
                servidor.send_message(ip_destino, puerto_destino, mensaje.to_dict())
                print("✅ Mensaje enviado!\n")
            
            elif comando == "test": 
                # AUTO-TEST
                print("\n🧪 Enviando mensaje de prueba a ti mismo...")
                mensaje = Message(
                    msg_type=MessageType. HEARTBEAT,
                    sender_id=f"nodo_puerto_{mi_puerto}",
                    data={"test": "auto-mensaje"}
                )
                servidor. send_message("127.0.0.1", mi_puerto, mensaje.to_dict())
                time.sleep(1)
            
            elif comando == "quit": 
                break
            
            else:
                print("❌ Comando no reconocido. Usa: enviar, test, quit")
    
    except KeyboardInterrupt: 
        print("\n\n👋 Ctrl+C detectado...")
    
    finally:
        servidor. stop()
        print("✅ Nodo cerrado.  ¡Hasta luego!")






if __name__ == "__main__":
    main()