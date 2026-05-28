import redis
import json
import os
import time
import pandas as pd

r = redis.Redis(host='redis', port=6379, decode_responses=True)

WORKER_TYPE = os.environ.get("WORKER_TYPE", "promedios")
WORKER_ID = os.environ.get("WORKER_ID", "1")

print(f"Worker {WORKER_ID} ({WORKER_TYPE}) iniciado, esperando tareas...")

def calcular_promedios(df):
    numericas = df.select_dtypes(include='number')
    resultado = numericas.mean().round(2).to_dict()
    return f"Promedios: {resultado}"

def calcular_totales(df):
    numericas = df.select_dtypes(include='number')
    resultado = numericas.sum().round(2).to_dict()
    return f"Totales: {resultado}"

def detectar_invalidos(df):
    invalidos = df.isnull().sum().to_dict()
    total = sum(invalidos.values())
    return f"Registros inválidos por columna: {invalidos} | Total: {total}"

def procesar(task):
    df = pd.read_csv(task["filepath"])
    if WORKER_TYPE == "promedios":
        return calcular_promedios(df)
    elif WORKER_TYPE == "totales":
        return calcular_totales(df)
    elif WORKER_TYPE == "invalidos":
        return detectar_invalidos(df)

while True:
    item = r.brpop("tasks", timeout=5)
    if item is None:
        continue

    task = json.loads(item[1])

    if task["worker_type"] != WORKER_TYPE:
        r.lpush("tasks", item[1])
        time.sleep(0.5)
        continue

    task_id = task["task_id"]
    key = f"status:{task_id}:{WORKER_TYPE}"

    print(f"Worker {WORKER_ID} procesando tarea {task_id}")
    r.hset(key, mapping={"status": "en proceso", "result": ""})

    try:
        time.sleep(2)
        resultado = procesar(task)
        r.hset(key, mapping={"status": "completada", "result": resultado})
        print(f"Worker {WORKER_ID} completó tarea {task_id}")
    except Exception as e:
        r.hset(key, mapping={"status": "error", "result": str(e)})
        print(f"Worker {WORKER_ID} error: {e}")