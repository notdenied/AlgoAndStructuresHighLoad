import re
from collections import defaultdict

class InvertedIndex:
    def __init__(self):
        # word -> set of report IDs
        self.index = defaultdict(set)
        # ID -> report content
        self.reports = {}

    def _tokenize(self, text):
        """Простая токенизация: очистка от пунктуации и приведение к нижнему регистру."""
        return re.findall(r'\w+', text.lower())

    def add_report(self, report_id, content):
        """Добавляет отчет в индекс."""
        self.reports[report_id] = content
        words = self._tokenize(content)
        for word in words:
            self.index[word].add(report_id)

    def search(self, query):
        """Поиск отчетов по ключевому слову за O(1)."""
        query_word = query.lower()
        report_ids = self.index.get(query_word, set())
        
        results = []
        for r_id in report_ids:
            results.append({
                "id": r_id,
                "content": self.reports[r_id]
            })
        return results


