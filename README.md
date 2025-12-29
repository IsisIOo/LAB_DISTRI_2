# LAB_DISTRI_2

# 1. Obtener IP real
ifconfig | grep "inet "  # Linux/Mac
# o ipconfig  # Windows → busca 192.168.x.x

# 2. Levantar nodo (puerto 15000)
docker build -t p2p-test .
docker run --rm -it -p 15000:5000 p2p-test bash

# DENTRO del contenedor:
python main.py

# Configuración:
Tu IP: 0.0.0.0
Tu puerto: 5000
¿Unirse?: n  ← PRIMER NODO

>>> status
# Debe mostrar: Successor: (192.168.1.100, 15000, abc123...)
>>> storage
# Vacío inicialmente
