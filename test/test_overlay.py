"""
Pruebas para el Módulo 3: Overlay (Chord)
Verifica hash, anillo, successor, predecessor
"""
import sys
import os
import pytest
import time

sys. path.insert(0, os. path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.overlay import ChordNode, create_node_id, is_key_in_range


class TestHash:
    """Pruebas de funciones de hash"""
    
    def test_create_node_id(self):
        """Verifica creación de ID de nodo"""
        node_id = create_node_id("192.168.1.1", 5000)
        
        assert isinstance(node_id, str)
        assert len(node_id) == 40  # SHA-1 = 40 caracteres hex
    
    def test_consistent_hash(self):
        """Verifica que el hash sea consistente"""
        id1 = create_node_id("192.168.1.1", 5000)
        id2 = create_node_id("192.168.1.1", 5000)
        
        assert id1 == id2
    
    def test_different_nodes_different_hash(self):
        """Verifica que nodos diferentes tengan hash diferente"""
        id1 = create_node_id("192.168.1.1", 5000)
        id2 = create_node_id("192.168.1.2", 5000)
        
        assert id1 != id2


class TestChordNode:
    """Pruebas para ChordNode"""
    
    def test_node_creation(self):
        """Verifica creación de nodo"""
        node = ChordNode("192.168.1.1", 5000)
        
        assert node.ip == "192.168.1.1"
        assert node.port == 5000
        assert len(node.node_id) == 40
        assert node.successor is not None
    
    def test_first_node_self_successor(self):
        """Primer nodo debe ser su propio successor"""
        node = ChordNode("127.0.0.1", 5000)
        
        # Al no unirse a nadie, debería apuntarse a sí mismo
        assert node. successor[2] == node.node_id
    
    def test_node_info(self):
        """Verifica get_node_info"""
        node = ChordNode("192.168.1.1", 5000)
        info = node.get_node_info()
        
        assert "node_id" in info
        assert "ip" in info
        assert "port" in info
        assert "successor" in info
        assert "predecessor" in info
    
    def test_calculate_hash(self):
        """Verifica cálculo de hash"""
        node = ChordNode("192.168.1.1", 5000)
        
        key_hash = node._calculate_hash("test_key")
        
        assert isinstance(key_hash, str)
        assert len(key_hash) == 40


class TestKeyRange:
    """Pruebas para is_key_in_range"""
    
    def test_normal_range(self):
        """Prueba rango normal (sin wraparound)"""
        # Rango: 20 < x <= 60
        assert is_key_in_range("30", "20", "60", inclusive_end=True)
        assert is_key_in_range("60", "20", "60", inclusive_end=True)
        assert not is_key_in_range("20", "20", "60", inclusive_end=True)
        assert not is_key_in_range("70", "20", "60", inclusive_end=True)
    
    def test_wraparound_range(self):
        """Prueba rango con wraparound (cruza el 0)"""
        # Convertir a hex para simular hashes
        start = "f" * 40  # Cerca del máximo
        end = "0" * 39 + "5"  # Cerca del mínimo
        
        # Valores que deberían estar en el rango
        in_range = "f" * 39 + "a"  # Mayor que start
        also_in_range = "0" * 39 + "3"  # Menor que end
        
        assert is_key_in_range(in_range, start, end, inclusive_end=True)
        assert is_key_in_range(also_in_range, start, end, inclusive_end=True)


class TestResponsibility:
    """Pruebas de responsabilidad de claves"""
    
    def test_get_responsible_node(self):
        """Verifica determinación de nodo responsable"""
        node = ChordNode("127.0.0.1", 5000)
        node.is_joined = True
        
        responsible = node.get_responsible_node("test_key")
        
        assert responsible is not None
        assert len(responsible) == 3  # (ip, port, node_id)