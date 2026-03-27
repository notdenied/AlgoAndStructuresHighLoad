import sys
import os
import shutil

# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task1.lsm_tree import FullLSMTree

def test_lsm_full_features():
    data_dir = "project/tests/test_lsm_storage"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    # Инициализация с маленькими лимитами для быстрой проверки
    tree = FullLSMTree(data_dir=data_dir, memtable_limit=3, sparse_idx_step=2)
    
    print("🚦 Тест 1: Запись и Flush")
    tree.put("key1", "value1")
    tree.put("key2", "value2")
    # key3 triggers flush
    print("Добавляем key3...")
    tree.put("key3", "value3") 
    
    val1 = tree.get("key1")
    print(f"Поиск key1: {val1}")
    assert val1 == "value1", f"Ожидалось value1, получено {val1}"
    assert len(tree.sstable_metadata) == 1, f"Ожидалось 1 SSTable, получено {len(tree.sstable_metadata)}"
    
    print("🚦 Тест 2: Удаление и WAL")
    tree.delete("key2")
    tree.put("key4", "value4")
    
    # Перезапуск для проверки WAL
    print("--- Перезапуск (Crash Simulation 1) ---")
    tree_rec1 = FullLSMTree(data_dir=data_dir, memtable_limit=3, sparse_idx_step=2)
    assert tree_rec1.get("key4") == "value4"
    assert tree_rec1.get("key2") is None
    assert tree_rec1.get("key1") == "value1"
    
    print("🚦 Тест 3: Наполнение и Компакция")
    tree_rec1.put("key5", "value5") # Триггер Flush
    tree_rec1.put("key6", "value6")
    
    print(f"SSTables before compact: {len(tree_rec1.sstable_metadata)}")
    tree_rec1.compact()
    print(f"SSTables after compact: {len(tree_rec1.sstable_metadata)}")
    
    assert len(tree_rec1.sstable_metadata) == 1, f"Ожидалось 1 SSTable после компакции, получено {len(tree_rec1.sstable_metadata)}"
    
    v1 = tree_rec1.get("key1")
    v3 = tree_rec1.get("key3")
    v6 = tree_rec1.get("key6")
    
    print(f"После компакции: key1={v1}, key3={v3}, key6={v6}")
    assert v1 == "value1", f"После компакции: ошибка key1, получено {v1}"
    assert v3 == "value3", f"После компакции: ошибка key3, получено {v3}"
    assert v6 == "value6", f"После компакции: ошибка key6, получено {v6}"
    
    # ФИНАЛЬНЫЙ ТЕСТ: Перезапуск после компакции
    print("--- Перезапуск (Crash Simulation 2) ---")
    tree_final = FullLSMTree(data_dir=data_dir, memtable_limit=3, sparse_idx_step=2)
    assert tree_final.get("key1") == "value1", f"Final: ошибка key1, получено {tree_final.get('key1')}"
    assert tree_final.get("key6") == "value6", f"Final: ошибка key6, получено {tree_final.get('key6')}"
    
    # Очистка
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        
    print("\n✅ Все тесты LSM-Tree пройдены успешно!")

if __name__ == "__main__":
    test_lsm_full_features()
