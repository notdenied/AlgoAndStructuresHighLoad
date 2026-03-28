import os
import json
import time
import hashlib
import re
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
    def __init__(self, data_dir="data/lsm_storage", memtable_limit=100, sparse_idx_step=10):
        self.data_dir = data_dir
        self.memtable_limit = memtable_limit
        self.sparse_idx_step = sparse_idx_step
        self.memtable = {}
        self.sstable_metadata = []
        self.wal_path = os.path.join(self.data_dir, "wal.log")
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self._load_metadata()
        self._recover_from_wal()

    def _load_metadata(self):
        files = sorted([f for f in os.listdir(self.data_dir) if f.endswith(".meta")])
        for meta_file in files:
            path = os.path.join(self.data_dir, meta_file)
            with open(path, 'r') as f:
                data = json.load(f)
                bloom = BloomFilter(size=data["bloom_size"], hash_count=data["bloom_hashes"])
                bloom.bit_array = data["bloom_bits"]
                self.sstable_metadata.append({"path": data["sstable_path"], "filter": bloom, "index": data["sparse_index"]})

    def _recover_from_wal(self):
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, 'r') as f:
            for line in f:
                try:
                    k, v = json.loads(line)
                    self.memtable[k] = v
                except:
                    continue
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

    def get(self, key):
        if key in self.memtable:
            val = self.memtable[key]
            return None if val == "__DELETED__" else val
        for meta in reversed(self.sstable_metadata):
            if not meta["filter"].maybe_contains(key):
                continue
            sparse_idx = meta["index"]
            candidate_keys = [k for k in sparse_idx.keys() if k <= key]
            if not candidate_keys:
                continue
            start_pos = sparse_idx[max(candidate_keys)]
            with open(meta["path"], 'r') as f:
                f.seek(start_pos)
                for line in f:
                    parts = line.strip().split(',', 1)
                    if len(parts) < 2:
                        continue
                    k, v = parts
                    if k == key:
                        return None if v == "__DELETED__" else v
                    if k > key:
                        break
        return None

    def _flush(self):
        if not self.memtable:
            return
        timestamp = int(time.time() * 1000000)
        filename = f"sstable_{timestamp}.txt"
        path = os.path.join(self.data_dir, filename)
        sorted_keys = sorted(self.memtable.keys())
        bloom, sparse_index = BloomFilter(), {}
        with open(path, 'w') as f:
            for i, k in enumerate(sorted_keys):
                pos = f.tell()
                f.write(f"{k},{self.memtable[k]}\n")
                bloom.add(k)
                if i % self.sparse_idx_step == 0:
                    sparse_index[k] = pos
        meta_data = {"sstable_path": path, "bloom_bits": bloom.bit_array, "bloom_size": bloom.size,
                     "bloom_hashes": bloom.hash_count, "sparse_index": sparse_index}
        with open(path + ".meta", 'w') as f:
            json.dump(meta_data, f)
        self.sstable_metadata.append({"path": path, "filter": bloom, "index": sparse_index})
        self.memtable = {}
        if os.path.exists(self.wal_path):
            os.remove(self.wal_path)


class AppendOnlyLog:
    def __init__(self, log_file="data/commands.log"):
        self.log_file = log_file

    def append(self, command):
        with open(self.log_file, "a") as f:
            f.write(json.dumps({"timestamp": int(time.time()), "command": command}) + "\n")

    def read_from(self, offset=0):
        if not os.path.exists(self.log_file):
            return [], 0
        commands, current_offset = [], 0
        with open(self.log_file, "r") as f:
            for i, line in enumerate(f):
                if i >= offset:
                    commands.append(json.loads(line))
                current_offset = i + 1
        return commands, current_offset


class InvertedIndex:
    def __init__(self):
        self.index = defaultdict(set)
        self.reports = {}

    def _tokenize(self, text):
        return re.findall(r'\w+', text.lower())

    def add_report(self, report_id, content):
        self.reports[report_id] = content
        for word in self._tokenize(content):
            self.index[word].add(report_id)

    def search(self, query):
        query_word = query.lower()
        report_ids = self.index.get(query_word, set())
        return [{"id": r_id, "content": self.reports[r_id]} for r_id in report_ids]
