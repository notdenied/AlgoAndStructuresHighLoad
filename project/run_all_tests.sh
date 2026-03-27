#!/bin/bash

echo "🚀 Запуск всех тестов проекта Smart Grid..."
echo "--------------------------------------------------"

# Устанавливаем PYTHONPATH, чтобы импорты работали корректно
export PYTHONPATH=$PYTHONPATH:$(pwd)/project

# 1. Тест LSM-Tree
echo "📦 Тестирование LSM-Tree..."
python3 project/tests/test_lsm.py
if [ $? -eq 0 ]; then
    echo "✅ LSM-Tree: Пройдено"
else
    echo "❌ LSM-Tree: Ошибка"
fi

echo "--------------------------------------------------"

# 2. Тест Inverted Index
echo "🔍 Тестирование Inverted Index..."
python3 project/tests/test_incident_index.py
if [ $? -eq 0 ]; then
    echo "✅ Inverted Index: Пройдено"
else
    echo "❌ Inverted Index: Ошибка"
fi

echo "--------------------------------------------------"

# 3. Тест Command Bus
echo "📡 Тестирование Command Bus..."
python3 project/tests/test_command_bus.py
if [ $? -eq 0 ]; then
    echo "✅ Command Bus: Пройдено"
else
    echo "❌ Command Bus: Ошибка"
fi

echo "--------------------------------------------------"

# 4. Тест MapReduce
echo "⚙️  Тестирование MapReduce (локально)..."
python3 project/tests/test_map_reduce.py
if [ $? -eq 0 ]; then
    echo "✅ MapReduce: Пройдено"
else
    echo "❌ MapReduce: Ошибка"
fi

echo "--------------------------------------------------"
echo "🏁 Все тесты завершены!"
