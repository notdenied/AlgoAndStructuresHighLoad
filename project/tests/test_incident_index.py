import sys
import os

# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task2.incident_index import InvertedIndex

def test_incident_index():
    idx = InvertedIndex()
    
    # Пример данных от аварийных бригад
    reports = [
        (101, "Короткое замыкание на фидере №5. Требуется замена изолятора."),
        (102, "Плановый осмотр трансформатора. Состояние стабильное."),
        (103, "Аварийное отключение: замыкание из-за падения ветки."),
        (104, "Замена кабеля на подстанции 'Северная'."),
    ]
    
    for rid, text in reports:
        idx.add_report(rid, text)
    
    print("Поиск по слову 'замыкание':")
    results = idx.search("замыкание")
    for res in results:
        print(f"  - [{res['id']}] {res['content']}")
    
    assert len(results) == 2, "Должно быть найдено 2 отчета для 'замыкание'"
        
    print("\nПоиск по слову 'замена':")
    results = idx.search("замена")
    for res in results:
        print(f"  - [{res['id']}] {res['content']}")
    
    assert len(results) == 2, "Должно быть найдено 2 отчета для 'замена'"
    print("\nТест Incident Index пройден успешно!")

if __name__ == "__main__":
    test_incident_index()
