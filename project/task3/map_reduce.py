import multiprocessing
import os
import csv
from collections import defaultdict


def mapper(chunk_path):
    """
    Чисто учебный Mapper: читает кусок CSV и выдает (sensor_id, power_in_watts).
    Мощность = Voltage * Current.
    """
    results = []
    try:
        with open(chunk_path, 'r') as f:
            reader = csv.reader(f)
            # Пропускаем заголовок, если он есть (в чанках его может не быть,
            # но мы будем делить файл умно)
            header = next(reader, None)
            if header and header[0] == 'timestamp':
                pass
            else:
                # Если первая строка не заголовок, обрабатываем ее
                if header:
                    results.append((header[1], float(header[2]) * float(header[3])))

            for row in reader:
                if len(row) < 4:
                    continue
                ts, s_id, v, c = row
                results.append((s_id, float(v) * float(c)))
    except Exception as e:
        print(f"Error in mapper {chunk_path}: {e}")
    return results


def reducer(mapped_items):
    """
    Reducer: суммирует значения для каждого ключа.
    """
    aggregated = defaultdict(float)
    for s_id, power in mapped_items:
        aggregated[s_id] += power
    return aggregated


def split_file(file_path, num_chunks):
    """Разбивает большой файл на чанки по строкам."""
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_chunks

    chunks = []
    with open(file_path, 'r') as f:
        # Пропускаем заголовок
        f.readline()

        for i in range(num_chunks):
            chunk_filename = f"project/task3/chunk_{i}.csv"
            with open(chunk_filename, 'w') as chunk_f:
                bytes_written = 0
                while bytes_written < chunk_size:
                    line = f.readline()
                    if not line:
                        break
                    chunk_f.write(line)
                    bytes_written += len(line)
            chunks.append(chunk_filename)
    return chunks


def run_map_reduce(input_file, num_workers=4):
    print(f"Запуск MapReduce для {input_file} с {num_workers} воркерами...")

    # 1. Сплит данных
    chunks = split_file(input_file, num_workers)

    # 2. Map (параллельно)
    with multiprocessing.Pool(num_workers) as pool:
        map_results = pool.map(mapper, chunks)

    # 3. Shuffle (собираем все результаты мапперов в один плоский список)
    # В полноценном MapReduce это более сложный шаг.
    all_mapped = []
    for partial_result in map_results:
        all_mapped.extend(partial_result)

    # 4. Reduce
    final_result = reducer(all_mapped)

    # Очистка чанков
    for c in chunks:
        os.remove(c)

    return final_result



