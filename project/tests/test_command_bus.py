import sys
import os

# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task2.command_bus import AppendOnlyLog

def test_command_bus():
    log_file = "project/tests/test_commands.log"
    if os.path.exists(log_file):
        os.remove(log_file)
        
    log = AppendOnlyLog(log_file)
    
    # 1. Диспетчер отправляет команды
    print("Диспетчер отправляет команды...")
    log.append("отключить фидер №5")
    log.append("включить резерв на подстанции 'Центр'")
    
    # 2. Счетчик (Sensor) читает новые команды
    print("\nСчетчик читает команды с offset 0:")
    new_cmds, offset = log.read_from(0)
    for cmd in new_cmds:
        print(f"  - {cmd['command']}")
    
    assert len(new_cmds) == 2, "Должно быть найдено 2 команды"
    assert offset == 2, "Offset должен быть 2"
        
    # 3. Диспетчер добавляет еще одну команду
    print("\nДиспетчер добавляет команду 'проверка связи'...")
    log.append("проверка связи")
    
    # 4. Счетчик читает только новые команды, используя запомненный offset
    print(f"\nСчетчик читает новые команды с offset {offset}:")
    latest_cmds, new_offset = log.read_from(offset)
    for cmd in latest_cmds:
        print(f"  - {cmd['command']}")
    
    assert len(latest_cmds) == 1, "Должна быть найдена 1 новая команда"
    assert latest_cmds[0]['command'] == "проверка связи"
    
    # Cleanup
    if os.path.exists(log_file):
        os.remove(log_file)
    print("\nТест Command Bus пройден успешно!")

if __name__ == "__main__":
    test_command_bus()
