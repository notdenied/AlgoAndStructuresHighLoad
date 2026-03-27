import json
import os
import time

class AppendOnlyLog:
    def __init__(self, log_file="commands.log"):
        self.log_file = log_file

    def append(self, command):
        """Запись команды в конец лога."""
        with open(self.log_file, "a") as f:
            entry = {
                "timestamp": int(time.time()),
                "command": command
            }
            f.write(json.dumps(entry) + "\n")

    def read_from(self, offset=0):
        """Чтение команд из лога, начиная с указанного смещения (байты или строки)."""
        # Для простоты — чтение по строкам
        if not os.path.exists(self.log_file):
            return [], 0

        commands = []
        current_offset = 0
        with open(self.log_file, "r") as f:
            for i, line in enumerate(f):
                if i >= offset:
                    commands.append(json.loads(line))
                    current_offset = i + 1
                else:
                    current_offset = i + 1
                    
        return commands, current_offset


