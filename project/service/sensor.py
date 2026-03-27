import requests
import time
import random
import os
import threading

SENSOR_ID = os.getenv("SENSOR_ID", "sensor_default")
COLLECTOR_URL = os.getenv("COLLECTOR_URL", "http://collector:8000")

class SmartSensor:
    def __init__(self, sensor_id, collector_url):
        self.sensor_id = sensor_id
        self.collector_url = collector_url
        self.offset = 0
        self.is_on = True
        self.running = True

    def send_telemetry(self):
        while self.running:
            if self.is_on:
                v = random.uniform(210, 230)
                c = random.uniform(5, 15)
                try:
                    payload = {"sensor_id": self.sensor_id, "voltage": v, "current": c}
                    requests.post(f"{self.collector_url}/telemetry", json=payload, timeout=2)
                    print(f"[{self.sensor_id}] Sent: {v:.1f}V, {c:.1f}A")
                except Exception as e:
                    print(f"[{self.sensor_id}] Telemetry error: {e}")
            else:
                print(f"[{self.sensor_id}] Sensor is OFF")
            time.sleep(2)

    def poll_commands(self):
        while self.running:
            try:
                # Передаем sensor_id, чтобы коллектор обновил статус last_seen
                resp = requests.get(f"{self.collector_url}/poll_commands?sensor_id={self.sensor_id}&offset={self.offset}", timeout=2)
                if resp.status_code == 200:
                    data = resp.json()
                    commands = data.get("commands", [])
                    self.offset = data.get("next_offset", self.offset)
                    
                    for cmd_entry in commands:
                        cmd_str = cmd_entry.get("command", "")
                        if ":" in cmd_str:
                            target, action = cmd_str.split(":", 1)
                            if target == self.sensor_id or target == "ALL":
                                if action == "ON":
                                    self.is_on = True
                                    print(f"[{self.sensor_id}] Switched ON")
                                elif action == "OFF":
                                    self.is_on = False
                                    print(f"[{self.sensor_id}] Switched OFF")
            except Exception as e:
                print(f"[{self.sensor_id}] Polling error: {e}")
            time.sleep(2)

    def report_incidents(self):
        """Эмуляция динамических логов ошибок."""
        errors = [
            "Критическое падение напряжения",
            "Перегрев трансформаторного узла",
            "Подозрение на несанкционированное подключение",
            "Сбой синхронизации фаз",
            "Высокий уровень гармоник в сети"
        ]
        while self.running:
            if random.random() < 0.5:  # 50% шанс ошибки каждые 4 сек (для демо)
                msg = random.choice(errors)
                try:
                    report_id = int(time.time() * 1000) % 1000000
                    payload = {"report_id": report_id, "content": f"[{self.sensor_id}] {msg}"}
                    requests.post(f"{self.collector_url}/reports", json=payload, timeout=2)
                    print(f"[{self.sensor_id}] Reported Incident: {msg}")
                except Exception as e:
                    print(f"[{self.sensor_id}] Reporting error: {e}")
            time.sleep(4)

    def start(self):
        t1 = threading.Thread(target=self.send_telemetry)
        t2 = threading.Thread(target=self.poll_commands)
        t3 = threading.Thread(target=self.report_incidents)
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()

if __name__ == "__main__":
    sensor = SmartSensor(SENSOR_ID, COLLECTOR_URL)
    print(f"Starting Sensor: {SENSOR_ID}")
    sensor.start()
