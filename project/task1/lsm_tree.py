import os


class MiniLSMTree:
    def __init__(self, max_memtable_size=5, max_sstable_count=5, data_dir="lsm_data"):
        """
        Инициализация структуры LSM-дерева.
        max_memtable_size - искусственно заниженный лимит ключей 
        для демонстрации работы flush() (в реальности это мегабайты).
        """
        self.max_memtable_size = max_memtable_size
        self.max_sstable_count = max_sstable_count

        # MemTable: структура данных в оперативной памяти (для поиска O(1) используем словарь)
        self.memtable = {}

        # Список файлов SSTable на диске (от старых к новым)
        self.sstable_files = []

        # Директория для хранения файлов
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        print("LSM-Tree DB инициализирована.")

    def put(self, key, value):
        """Запись данных. Происходит мгновенно, так как пишем только в ОЗУ."""
        self.memtable[key] = value

        print(f"PUT: {key} -> {value} (в MemTable)")

        if len(self.memtable) > self.max_memtable_size:
            self.flush()

    def flush(self):
        """Сброс оперативной памяти на диск (создание SSTable)."""
        if not self.memtable:
            return

        sstable_idx = len(self.sstable_files)
        filepath = os.path.join(self.data_dir, f"sstable_{str(sstable_idx).zfill(6)}.txt")

        sorted_keys = list(sorted(self.memtable.keys()))

        # Записываем отсортированные данные на диск
        with open(filepath, 'w') as f:
            for k in sorted_keys:
                f.write(f"{k},{self.memtable[k]}\n")

        self.sstable_files.append(filepath)
        print(f"\n[FLUSH] MemTable переполнен. Данные сброшены в диск: {filepath}")

        self.memtable = {}

        if len(self.sstable_files) > self.max_sstable_count:
            self.compact()

    def get(self, key):
        """Поиск ключа. Это самое узкое (медленное) место LSM-деревьев."""
        print(f"\nGET: Поиск ключа '{key}'...")

        value = self.memtable.get(key, None)
        if value:
            return value

        for filepath in sorted(next(os.walk(self.data_dir))[-1], reverse=True):
            with open(f'{self.data_dir}/{filepath}', 'r') as f:
                for line in f:
                    k, v = line.strip().split(',')
                    if k == str(key):
                        print(f"Найдено на диске (история): {filepath}")
                        return v

        print("Ключ не найден.")
        return None

    def compact(self):
        # TODO: compaction
        ...
