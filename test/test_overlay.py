"""
Tests de Overlay :  Calculo de hash, determinacion de successor y predecessor, operacion
JOIN.
"""
import os
import sys
import hashlib
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.overlay import ChordNode

#pruebas de la función de hash SHA-1.
class TestHashBasics:

    """test_calculo_hash_sha1
    descripcion:verifica que el hash SHA-1 sea consistente.
    entrada:texto de entrada para hash
    salida:-"""
    def test_calculo_hash_sha1(self):
        texto = "192.168.1.1:5000"
        esperado = hashlib.sha1(texto.encode()).hexdigest()

        nodo = ChordNode("192.168.1.1", 5000)
        obtenido = nodo._calculate_hash(texto)

        assert isinstance(obtenido, str)
        assert len(obtenido) == 40
        assert obtenido == esperado


#pruebas para successor y predecessor
class TestSuccessorPredecessor:

    """test_primer_nodo_se_autoreferencia
    descripcion:Verifica que el primer nodo apunte su successor a sí mismo y predecessor None
    entrada:-
    salida:-"""
    def test_primer_nodo_se_autoreferencia(self):
        """El primer nodo debe apuntar su successor a sí mismo y predecessor None."""
        nodo = ChordNode("127.0.0.1", 6000)
        assert nodo.is_joined is True
        assert nodo.successor is not None
        assert nodo.successor[2] == nodo.node_id
        assert nodo.predecessor is None

    """test_find_successor_con_un_solo_nodo
    descripcion:Verifica que con un solo nodo en el anillo, el successor para cualquier
    clave es el mismo nodo.
    entrada:-
    salida:-"""
    def test_find_successor_con_un_solo_nodo(self):
        nodo = ChordNode("127.0.0.1", 6001)
        clave = "test_key"
        key_hash = nodo._calculate_hash(clave)
        succ = nodo.find_successor(key_hash)
        assert succ is not None
        assert succ[2] == nodo.successor[2]

#pruebas para la operación JOIN
class TestJoinOperation:

    """test_establece_successor
    descripcion:Verifica que un nodo que se une a la red establece correctamente su successor.
    entrada:-
    salida:-"""
    def test_establece_successor(self):
        bootstrap_ip, bootstrap_port = "127.0.0.1", 7000 # Nodo bootstrap
        n_boot = ChordNode(bootstrap_ip, bootstrap_port) # Nodo bootstrap

        n2_ip, n2_port = "127.0.0.1", 7001 # Nuevo nodo
        n2 = ChordNode(n2_ip, n2_port) # Nodo que se une
        unido = n2.join_network((bootstrap_ip, bootstrap_port)) # Unirse al anillo

        assert unido is True
        assert n2.is_joined is True
        assert n2.successor is not None
        esperado_id_boot = hashlib.sha1(f"{bootstrap_ip}:{bootstrap_port}".encode()).hexdigest()
        assert n2.successor[2] == esperado_id_boot

    """test_join_request_responde_con_successor
    descripcion:Verifica que el nodo bootstrap responde correctamente a una solicitud JOIN.
    entrada:-
    salida:-"""
    def test_join_request_responde_con_successor(self):
        bootstrap = ChordNode("127.0.0.1", 7100)

        # simulamos nuevo nodo que quiere unirse al anillo
        nuevo_ip, nuevo_port = "127.0.0.1", 7101
        nuevo_id = hashlib.sha1(f"{nuevo_ip}:{nuevo_port}".encode()).hexdigest()
        mensaje = { #tipo de mensaje JOIN
            "type": "CHORD_JOIN_REQUEST",
            "node_id": nuevo_id,
            "ip": nuevo_ip,
            "port": nuevo_port,
        }

        respuesta = bootstrap.handle_message(mensaje)
        assert isinstance(respuesta, dict)
        assert respuesta.get("type") == "JOIN_RESPONSE"
        # como en el anillo solo está el bootstrap, su successor es el mismo
        assert respuesta.get("successor_id") == bootstrap.node_id
        assert respuesta.get("successor_ip") == bootstrap.ip
        assert respuesta.get("successor_port") == bootstrap.port
