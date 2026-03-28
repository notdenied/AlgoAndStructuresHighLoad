from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
import os
import json
import time

from core import FullLSMTree, AppendOnlyLog, InvertedIndex


app = FastAPI(title="Smart Grid Collector")

# Инициализация хранилищ
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

lsm = FullLSMTree(data_dir=os.path.join(DATA_DIR, "lsm_storage"))
command_log = AppendOnlyLog(log_file=os.path.join(DATA_DIR, "commands.log"))
inverted_index = InvertedIndex()

# Статус датчиков в памяти: {sensor_id: {"is_on": bool, "last_seen": int}}
sensors_status = {}

# Модели данных


class TelemetryData(BaseModel):
    sensor_id: str
    voltage: float
    current: float


class CommandRequest(BaseModel):
    target_sensor: str
    action: str  # e.g., "ON", "OFF"


class ReportRequest(BaseModel):
    report_id: int
    content: str


def update_sensor_status(sensor_id: str, is_on: Optional[bool] = None, last_val: Optional[str] = None):
    if sensor_id not in sensors_status:
        sensors_status[sensor_id] = {"is_on": True, "last_seen": 0, "last_val": "N/A"}
    sensors_status[sensor_id]["last_seen"] = int(time.time())
    if is_on is not None:
        sensors_status[sensor_id]["is_on"] = is_on
    if last_val is not None:
        sensors_status[sensor_id]["last_val"] = last_val


@app.post("/telemetry")
async def post_telemetry(data: TelemetryData):
    val_str = f"{data.voltage:.1f}V / {data.current:.1f}A"
    update_sensor_status(data.sensor_id, last_val=val_str)
    lsm.put(data.sensor_id, f"{data.voltage},{data.current}")
    return {"status": "ok"}


@app.post("/commands")
async def post_command(cmd: CommandRequest):
    full_command = f"{cmd.target_sensor}:{cmd.action}"
    command_log.append(full_command)
    # Предварительно обновляем статус, если команда широковещательная или мы уверены в ней
    # Но лучше дождаться подтверждения от датчика в его следующем поллинге
    return {"status": "cmd_queued", "command": full_command}


@app.get("/poll_commands")
async def poll_commands(sensor_id: str, offset: int = 0):
    # Датчик сообщает свой ID при поллинге, чтобы мы знали, что он жив
    update_sensor_status(sensor_id)
    # Если в логе команд есть переключение для этого датчика, оно придет в списке cmds
    cmds, next_offset = command_log.read_from(offset)

    # Мы можем проанализировать cmds здесь, чтобы обновить локальный статус is_on,
    # но лучше пусть датчик сам присылает свой статус (или мы парсим лог команд)
    for entry in cmds:
        target, action = entry["command"].split(":", 1)
        if target == sensor_id or target == "ALL":
            sensors_status[sensor_id]["is_on"] = (action == "ON")

    return {"commands": cmds, "next_offset": next_offset}


@app.get("/keywords")
async def get_keywords():
    return list(inverted_index.index.keys())


@app.get("/sensors")
async def get_sensors():
    return sensors_status


@app.post("/reports")
async def post_report(report: ReportRequest):
    inverted_index.add_report(report.report_id, report.content)
    return {"status": "indexed"}


@app.get("/search")
async def search_reports(q: str = Query(..., min_length=1)):
    results = inverted_index.search(q)
    return {"results": results}


@app.get("/stats")
async def get_stats():
    return {
        "sstables_count": len(lsm.sstable_metadata),
        "memtable_size": len(lsm.memtable),
        "reports_count": len(inverted_index.reports)
    }

# Dashboard


@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
