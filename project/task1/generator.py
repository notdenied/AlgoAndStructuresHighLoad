import time
import random


OUTPUT_FILE = "telemetry.csv"
TARGET_SIZE_GB = 0.1          # сколько генерить (None = бесконечно)
BATCH_SIZE = 10000          # строк за одну запись
NUM_SENSORS = 500_000       # количество сенсоров

# диапазоны значений
VOLTAGE_RANGE = (210.0, 240.0)
CURRENT_RANGE = (0.0, 100.0)


def generate_batch():
    """Генерирует пачку строк CSV"""
    ts = int(time.time())
    lines = []

    for _ in range(BATCH_SIZE):
        sensor_id = random.randint(1, NUM_SENSORS)
        voltage = random.uniform(*VOLTAGE_RANGE)
        current = random.uniform(*CURRENT_RANGE)

        # без f-string внутри append — быстрее через join
        lines.append(f"{ts},{sensor_id},{voltage:.2f},{current:.2f}\n")

    return "".join(lines)


def main():
    bytes_written = 0
    target_bytes = None

    if TARGET_SIZE_GB is not None:
        target_bytes = TARGET_SIZE_GB * 1024**3

    with open(OUTPUT_FILE, "w", buffering=16 * 1024 * 1024) as f:
        # заголовок (опционально)
        f.write("timestamp,sensor_id,voltage,current\n")

        while True:
            batch = generate_batch()
            f.write(batch)

            bytes_written += len(batch)

            # прогресс каждые ~100MB
            if bytes_written % (100 * 1024 * 1024) < len(batch):
                print(f"Written: {bytes_written / (1024**3):.2f} GB")

            if target_bytes and bytes_written >= target_bytes:
                print("Done.")
                break


if __name__ == "__main__":
    main()
