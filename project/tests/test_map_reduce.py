import sys
import os

# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task3.map_reduce import run_map_reduce

def test_map_reduce():
    input_csv = "project/task1/telemetry.csv"
    
    if not os.path.exists(input_csv):
        print(f"Файл {input_csv} не найден. Пропускаем тест.")
        return

    results = run_map_reduce(input_csv)
    
    print("\nТоп-5 датчиков по энергопотреблению (Вт*с):")
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
    for s_id, total_power in sorted_results[:5]:
        print(f"  Sensor {s_id}: {total_power:.2f} Ws")
    
    assert len(results) > 0, "Результаты MapReduce не должны быть пустыми"
    print("\nТест MapReduce пройден успешно!")

if __name__ == "__main__":
    test_map_reduce()
