from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import uuid
import os
import asyncio

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

    return {"task_id": task_id}

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    async def event_stream():
        while True:
            data = {}
            for worker_type in ["promedios", "totales", "invalidos"]:
                info = r.hgetall(f"status:{task_id}:{worker_type}")
                data[worker_type] = info
            
            yield f"data: {json.dumps(data)}\n\n"
            
            all_done = all(
                data[wt].get("status") in ["completada", "error"]
                for wt in ["promedios", "totales", "invalidos"]
            )
            if all_done:
                break
            await asyncio.sleep(1)

    return StreamingResponse(event_stream(), media_type="text/event-stream")