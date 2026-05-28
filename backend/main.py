from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import uuid
import os
import asyncio
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r = redis.Redis(host='redis', port=6379, decode_responses=True)

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    task_id = str(uuid.uuid4())

    content = await file.read()
    filepath = f"/app/uploads/{task_id}.csv"
    os.makedirs("/app/uploads", exist_ok=True)
    with open(filepath, "wb") as f:
        f.write(content)

    for worker_type in ["promedios", "totales", "invalidos"]:
        task = {
            "task_id": task_id,
            "worker_type": worker_type,
            "filepath": filepath,
            "status": "pendiente"
        }
        r.lpush("tasks", json.dumps(task))
        r.hset(f"status:{task_id}:{worker_type}", mapping={"status": "pendiente", "result": ""})

        # Registrar log del evento
        log_entry = json.dumps({
            "time": datetime.now().strftime("%H:%M:%S"),
            "worker": worker_type,
            "msg": f"Tarea {task_id[:8]}... encolada"
        })
        r.lpush("dashboard:logs", log_entry)
        r.ltrim("dashboard:logs", 0, 49)  # Mantener solo los últimos 50 logs

    return {"task_id": task_id}

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    async def event_stream():
        prev = {}
        while True:
            data = {}
            for worker_type in ["promedios", "totales", "invalidos"]:
                info = r.hgetall(f"status:{task_id}:{worker_type}")
                data[worker_type] = info

                # Registrar cambios de estado en el log
                new_status = info.get("status", "")
                old_status = prev.get(worker_type, "")
                if new_status != old_status and new_status:
                    log_entry = json.dumps({
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "worker": worker_type,
                        "msg": f"Estado → {new_status}"
                    })
                    r.lpush("dashboard:logs", log_entry)
                    r.ltrim("dashboard:logs", 0, 49)
                    prev[worker_type] = new_status

            yield f"data: {json.dumps(data)}\n\n"

            all_done = all(
                data[wt].get("status") in ["completada", "error"]
                for wt in ["promedios", "totales", "invalidos"]
            )
            if all_done:
                break
            await asyncio.sleep(1)

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.get("/dashboard")
async def get_dashboard():
    """Endpoint SSE para el dashboard en tiempo real"""
    async def dashboard_stream():
        while True:
            workers_info = []
            for i, worker_type in enumerate(["promedios", "totales", "invalidos"], 1):
                # Buscar la tarea más reciente de este worker
                keys = r.keys(f"status:*:{worker_type}")
                status = "inactivo"
                if keys:
                    # Tomar la clave más reciente
                    latest = keys[-1]
                    info = r.hgetall(latest)
                    status = info.get("status", "inactivo")

                workers_info.append({
                    "id": i,
                    "type": worker_type,
                    "status": status
                })

            # Obtener últimos logs
            raw_logs = r.lrange("dashboard:logs", 0, 19)
            logs = [json.loads(l) for l in raw_logs]

            # Contar tareas en la cola
            queue_size = r.llen("tasks")

            payload = {
                "workers": workers_info,
                "logs": logs,
                "queue_size": queue_size
            }

            yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(dashboard_stream(), media_type="text/event-stream")