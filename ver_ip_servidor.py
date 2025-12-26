import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
ip = s.getsockname()[0]
s.close()

print(f"""
╔══════════════════════════════════════════════════════════╗
║         INFORMACIÓN DEL SERVIDOR (PC A)                  ║
╚══════════════════════════════════════════════════════════╝

📍 IP del servidor:   {ip}
🔌 Puerto:  5000

💡 Usa estos datos en PC B (cliente):
   IP: {ip}
   Puerto:  5000
""")