import os
import json
import time
import hashlib
from collections import defaultdict


class BloomFilter:
    def __init__(self, size=1000, hash_count=3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = 0

    def _hashes(self, item):
        for i in range(self.hash_count):
            yield int(hashlib.md5(f"{item}-{i}".encode()).hexdigest(), 16) % self.size

    def add(self, item):
        for h in self._hashes(item):
            self.bit_array |= (1 << h)

    def maybe_contains(self, item):
        for h in self._hashes(item):
            if not (self.bit_array & (1 << h)):
                return False
        return True


class FullLSMTree:
    def __init__(self, data_dir="lsm_storage", memtable_limit=100, sparse_idx_step=10):
        self.data_dir = data_dir
        self.memtable_limit = memtable_limit
        self.sparse_idx_step = sparse_idx_step

        self.memtable = {}
        self.sstable_metadata = []  # List of dicts: {path, filter, index}
        self.wal_path = os.path.join(self.data_dir, "wal.log")

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self._load_metadata()
        self._recover_from_wal()
        print(f"LSM-Tree initialized in {data_dir}")

    def _load_metadata(self):
        """Загрузка метаданных для существующих SSTables."""
        files = sorted([f for f in os.listdir(self.data_dir) if f.endswith(".meta")])
        for meta_file in files:
            path = os.path.join(self.data_dir, meta_file)
            with open(path, 'r') as f:
                data = json.load(f)
                bloom = BloomFilter(size=data["bloom_size"], hash_count=data["bloom_hashes"])
                bloom.bit_array = data["bloom_bits"]

                self.sstable_metadata.append({
                    "path": data["sstable_path"],
                    "filter": bloom,
                    "index": data["sparse_index"]
                })
        print(f"Loaded {len(self.sstable_metadata)} SSTables metadata.")

    def _recover_from_wal(self):
        """Восстановление данных из WAL при запуске."""
        if not os.path.exists(self.wal_path):
            return

        print("Recovering from WAL...")
        with open(self.wal_path, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    k, v = json.loads(line)
                    self.memtable[k] = v
                except json.JSONDecodeError:
                    continue

        # Если MemTable после восстановления большой — сбрасываем на диск
        if len(self.memtable) >= self.memtable_limit:
            self._flush()

    def _write_to_wal(self, key, value):
        with open(self.wal_path, 'a') as f:
            f.write(json.dumps([key, value]) + "\n")

    def put(self, key, value):
        self._write_to_wal(key, value)
        self.memtable[key] = value

        if len(self.memtable) >= self.memtable_limit:
            self._flush()

    def delete(self, key):
        """Удаление через Tombstone."""
        self.put(key, "__DELETED__")

    def _flush(self):
        if not self.memtable:
            return

        timestamp = int(time.time() * 1000000) # Use microseconds for better precision
        filename = f"sstable_{timestamp}.txt"
        path = os.path.join(self.data_dir, filename)
        meta_path = path + ".meta"

        sorted_keys = sorted(self.memtable.keys())
        bloom = BloomFilter()
        sparse_index = {}

        with open(path, 'w') as f:
            for i, k in enumerate(sorted_keys):
                pos = f.tell()
                v = self.memtable[k]

                f.write(f"{k},{v}\n")
                bloom.add(k)

                if i % self.sparse_idx_step == 0:
                    sparse_index[k] = pos

        # Сохранение метаданных
        meta_data = {
            "sstable_path": path,
            "bloom_bits": bloom.bit_array,
            "bloom_size": bloom.size,
            "bloom_hashes": bloom.hash_count,
            "sparse_index": sparse_index
        }
        with open(meta_path, 'w') as f:
            json.dump(meta_data, f)

        self.sstable_metadata.append({
            "path": path,
            "filter": bloom,
            "index": sparse_index
        })

        # Очистка MemTable и WAL
        self.memtable = {}
        if os.path.exists(self.wal_path):
            os.remove(self.wal_path)

        print(f"Flushed to {path}")

    def get(self, key):
        # 1. MemTable
        if key in self.memtable:
            val = self.memtable[key]
            return None if val == "__DELETED__" else val

        # 2. SSTables (от новых к старым)
        for meta in reversed(self.sstable_metadata):
            # Bloom Filter check
            if not meta["filter"].maybe_contains(key):
                continue

            # Sparse Index lookup
            sparse_idx = meta["index"]
            # Находим ближайший ключ в индексе, который <= искомому
            candidate_keys = [k for k in sparse_idx.keys() if k <= key]
            if not candidate_keys:
                continue

            start_pos = sparse_idx[max(candidate_keys)]

            with open(meta["path"], 'r') as f:
                f.seek(start_pos)
                for line in f:
                    if not line.strip():
                        continue
                    parts = line.strip().split(',', 1)
                    if len(parts) < 2:
                        continue
                    k, v = parts
                    if k == key:
                        return None if v == "__DELETED__" else v
                    if k > key:  # Т.к. файл отсортирован
                        break

        return None

    def compact(self):
        """
        Простая компакция: объединение всех SSTables в один новый уровень.
        """
        self._flush() # Убеждаемся, что всё из памяти сброшено на диск
        if len(self.sstable_metadata) < 2:
            return

        print("Starting Compaction...")
        merged = {}
        # Читаем от старых к новым, чтобы новые перезаписывали старые
        for meta in self.sstable_metadata:
            with open(meta["path"], 'r') as f:
                for line in f:
                    parts = line.strip().split(',', 1)
                    if len(parts) == 2:
                        k, v = parts
                        merged[k] = v

        # Удаляем старые файлы и их метаданные
        for meta in self.sstable_metadata:
            if os.path.exists(meta["path"]):
                os.remove(meta["path"])
            meta_path = meta["path"] + ".meta"
            if os.path.exists(meta_path):
                os.remove(meta_path)

        self.sstable_metadata = []

        # Записываем как один новый MemTable (временно) и вызываем flush
        self.memtable = merged
        self._flush()
        print("Compaction complete.")



