# 📊 CSV Processor

Aplicación web de procesamiento distribuido de archivos CSV mediante Workers Docker, colas de mensajes con Redis y comunicación en tiempo real con SSE (Server-Sent Events).


## ¿Qué hace esta aplicación?

El usuario sube un archivo CSV desde el navegador. El sistema lo envía a un backend que lo encola en Redis y lo distribuye a **3 workers independientes** que procesan el archivo en paralelo, cada uno realizando una tarea diferente:

| Worker | Tarea |
|--------|-------|
| Worker 1 — Promedios | Calcula el promedio de cada columna numérica |
| Worker 2 — Totales | Suma todos los valores de cada columna numérica |
| Worker 3 — Inválidos | Detecta celdas vacías o con datos incorrectos |

Los resultados se muestran en tiempo real en la interfaz mientras los workers procesan. El historial de análisis se guarda localmente en el navegador con `localStorage`.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────┐
│                   NAVEGADOR                          │
│           Frontend (HTML + CSS + JS)                 │
│   - Drag & Drop de archivos CSV                      │
│   - Fetch API → POST /upload                         │
│   - SSE → GET /status/{task_id}                      │
│   - localStorage para historial                      │
└────────────────────┬────────────────────────────────┘
                     │ HTTP
┌────────────────────▼────────────────────────────────┐
│              Backend (FastAPI - Python)              │
│   - Recibe el CSV y lo guarda                        │
│   - Crea 3 tareas y las mete a la cola Redis         │
│   - Expone endpoint SSE para estado en tiempo real   │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              Cola de Mensajes (Redis)                │
│   - Lista "tasks" con las tareas pendientes          │
│   - Hashes "status:{task_id}:{worker}" con estado   │
└──────┬─────────────┬──────────────┬─────────────────┘
       │             │              │
┌──────▼──┐    ┌─────▼───┐   ┌─────▼──────┐
│Worker 1 │    │Worker 2 │   │ Worker 3   │
│Promedios│    │Totales  │   │ Inválidos  │
│(Docker) │    │(Docker) │   │ (Docker)   │
└─────────┘    └─────────┘   └────────────┘
```

---

## Tecnologías utilizadas

- **Frontend:** HTML5, CSS3, JavaScript 
- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Cola de mensajes:** Redis
- **Workers:** Python 3.11 (pandas, redis-py)
- **Contenedores:** Docker, Docker Compose
- **Comunicación en tiempo real:** SSE (Server-Sent Events)
- **Almacenamiento local:** localStorage (historial de análisis)

---

## 📁 Estructura del proyecto

```
csv-processor/
├── backend/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── frontend/
│   └── index.html
├── worker/
│   ├── Dockerfile
│   ├── worker.py
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## Cómo ejecutar el proyecto

### Requisitos previos
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo

### Pasos

**1. Clonar el repositorio:**
```bash
git clone https://github.com/BrisaAguayo/CSV-Processor.git
cd CSV-Processor
```

**2. Levantar todos los servicios con Docker Compose:**
```bash
docker-compose up --build
```

Esto levanta automáticamente:
- Redis en el puerto 6379
- Backend (FastAPI) en el puerto 8000
- 3 Workers procesando tareas de la cola

**3. Abrir el frontend:**

Abrir el archivo `frontend/index.html` directamente en el navegador.

**4. Usar la aplicación:**
- Arrastra un archivo `.csv` al cuadro o haz clic para seleccionarlo
- Presiona **Procesar CSV**
- Observa cómo los 3 workers procesan en paralelo en tiempo real
- Guarda el resultado para consultarlo después desde el historial

---

## ¿Cómo funciona la cola de mensajes?

Redis actúa como intermediario entre el backend y los workers:

1. El backend recibe el CSV y crea **3 tareas** (una por worker), las cuales empuja a una lista Redis llamada `tasks` usando `LPUSH`.
2. Cada worker ejecuta un loop continuo haciendo `BRPOP` sobre la lista `tasks`, lo que significa que **espera bloqueado** hasta que llegue una tarea.
3. Cuando llega una tarea, el worker la toma, actualiza el estado a `en proceso` en un hash Redis (`status:{task_id}:{worker_type}`), procesa el CSV y finalmente actualiza el estado a `completada` con el resultado.
4. El backend tiene un endpoint SSE que consulta esos hashes cada segundo y transmite el estado al navegador en tiempo real.
<img width="1093" height="632" alt="image" src="https://github.com/user-attachments/assets/9668bba5-6325-4ad5-9e07-1c6a503fb8cd" />


---

## Endpoints del backend

| Método | Ruta | Descripción |
|--------|------|-------------|
| `POST` | `/upload` | Recibe el CSV, crea las tareas en Redis |
| `GET` | `/status/{task_id}` | Stream SSE con el estado de los 3 workers |

---

## Funcionalidades del frontend

- **Drag & Drop** de archivos CSV
- **Fetch API** para enviar el archivo al backend
- **SSE** para recibir actualizaciones en tiempo real sin recargar la página
- **localStorage** para guardar y consultar el historial de análisis
- **Animaciones CSS** en carga de página, tarjetas y resultados
- **Spinner** animado durante el procesamiento

# 🛠️ Decisiones Técnicas y Explicación del Código

Este documento complementa el README principal con una explicación detallada de cada decisión técnica tomada en el proyecto.

---

## ¿Por qué Redis como cola de mensajes?

Redis fue elegido sobre otras opciones (RabbitMQ, Kafka, AWS SQS) por tres razones:

1. **Simplicidad** — se levanta con una sola línea en docker-compose, sin configuración extra.
2. **Velocidad** — vive en RAM, no en disco, por lo que las operaciones son casi instantáneas.
3. **Integración con Docker** — la imagen oficial `redis:alpine` es ligera y funciona perfectamente en la red interna de Docker Compose.

La cola funciona con dos comandos de Redis:
- `LPUSH` — el backend mete tareas al inicio de la lista `tasks`
- `BRPOP` — cada worker saca tareas del final de la lista, bloqueándose si no hay nada

Esto garantiza que cada tarea sea procesada exactamente una vez, por exactamente un worker.

---

## ¿Por qué 3 workers independientes en vez de uno?

La decisión de tener 3 workers separados (promedios, totales, inválidos) en lugar de un solo worker que haga todo tiene dos ventajas:

1. **Paralelismo real** — los 3 procesan al mismo tiempo, en vez de uno por uno. Si cada tarea tarda 2 segundos, con un worker tardaría 6 segundos en total; con 3 workers tarda 2 segundos.
2. **Separación de responsabilidades** — cada worker hace una sola cosa. Si uno falla, los otros siguen funcionando.

Los 3 workers usan exactamente el mismo código (`worker.py`). La diferencia es la variable de entorno `WORKER_TYPE` definida en `docker-compose.yml`. Esto evita duplicar código.

---

## ¿Por qué SSE y no WebSockets?

Para las actualizaciones en tiempo real se eligió SSE (Server-Sent Events) sobre WebSockets porque:

- **SSE es unidireccional** — el servidor manda datos al cliente, pero el cliente no necesita mandar datos de vuelta. Eso es exactamente lo que necesitamos: el backend manda el estado, el frontend solo lo muestra.
- **SSE es más simple** — no requiere librerías extra, está soportado nativamente en el navegador con `EventSource`.
- **WebSockets** son para comunicación bidireccional (chat, juegos en tiempo real), lo cual sería innecesariamente complejo para este caso.

---

## ¿Por qué FastAPI y no Flask o Express?

FastAPI fue elegido sobre Flask (Python) o Express (Node.js) por:

1. **Async nativo** — FastAPI está construido sobre `asyncio`, lo que significa que puede manejar múltiples conexiones SSE simultáneas sin bloquearse. Flask tradicional no soporta esto sin configuración extra.
2. **Menos código** — los decoradores `@app.post` y `@app.get` hacen el código muy legible.
3. **Alineación con el proyecto** — el worker ya usa Python (pandas), entonces mantener el backend en Python reduce la cantidad de lenguajes en el proyecto.

El uso de `async/await` y `asyncio.sleep` en vez de `time.sleep` es crítico: permite que el servidor atienda otras peticiones mientras espera, en vez de bloquear todo el proceso.

---

## ¿Por qué `selectedFile` en el frontend?

El input de tipo `file` en HTML es de **solo lectura** por razones de seguridad del navegador. No se puede asignar un archivo externo directamente con `input.files = archivo`.

Cuando el usuario arrastra un archivo, el archivo llega en `e.dataTransfer.files[0]`, no en el input. La solución fue crear una variable global `selectedFile` que guarda el archivo sin importar si vino del drag & drop o del explorador de archivos. El botón siempre usa `selectedFile` en vez de `input.files[0]`.

---

## ¿Por qué localStorage para el historial?

El historial de análisis se guarda en `localStorage` del navegador porque:

1. **No requiere base de datos** — simplificar la arquitectura. No necesitamos un servidor extra solo para guardar historial.
2. **Persiste entre sesiones** — a diferencia de una variable JavaScript, `localStorage` sobrevive si cierras el navegador.
3. **Es suficiente para este caso** — el historial es personal del usuario en ese navegador, no necesita sincronizarse con otros dispositivos.

Se limita a 10 entradas con `.slice(0, 10)` para no ocupar demasiado espacio.

---

## ¿Por qué volumes en docker-compose?

```yaml
volumes:
  - ./uploads:/app/uploads
```

El backend guarda los archivos CSV en `/app/uploads` dentro de su contenedor. Los workers necesitan leer esos mismos archivos. Sin `volumes`, cada contenedor tiene su propio sistema de archivos aislado y los workers nunca podrían ver los archivos que subió el backend.

El volumen conecta una carpeta real de la computadora (`./uploads`) con la carpeta interna del contenedor (`/app/uploads`), haciendo que todos los contenedores compartan el mismo espacio de archivos.

---

## ¿Por qué `depends_on: redis`?

Si el backend o los workers arrancaran antes que Redis, intentarían conectarse y fallarían porque Redis todavía no está listo. `depends_on` le dice a Docker Compose que espere a que Redis esté corriendo antes de arrancar los demás servicios.

---

## ¿Por qué el worker devuelve la tarea a la cola si no es suya?

```python
if task["worker_type"] != WORKER_TYPE:
    r.lpush("tasks", item[1])
    time.sleep(0.5)
    continue
```

Cuando un worker saca una tarea de la cola y ve que no es para él, la regresa con `lpush` para que el worker correcto la tome. El `time.sleep(0.5)` evita un loop demasiado rápido que consumiría CPU innecesariamente.

Una alternativa más eficiente sería tener 3 colas separadas (`tasks:promedios`, `tasks:totales`, `tasks:invalidos`), pero se eligió una sola cola por simplicidad.

---

## ¿Por qué el dashboard usa un snapshot para evitar parpadeo?

```javascript
const snapshot = JSON.stringify(data.logs);
if (snapshot !== lastLogSnapshot) {
    lastLogSnapshot = snapshot;
    // re-renderizar
}
```

El SSE del dashboard manda datos cada segundo. Si re-renderizáramos el log completo cada segundo aunque no hubiera cambios, el DOM se estaría reconstruyendo constantemente causando parpadeo visual. La solución fue comparar el contenido actual con el anterior (como "fotografía" en texto) y solo actualizar cuando realmente cambió algo.

---

## Flujo completo del sistema

```
1. Usuario arrastra CSV → se guarda en selectedFile
2. Click en "Procesar CSV" → fetch POST /upload con FormData
3. Backend recibe el archivo → lo guarda en /app/uploads
4. Backend crea 3 tareas → las mete en Redis con LPUSH
5. Backend inicializa estados → status:taskId:promedios = "pendiente"
6. Frontend abre SSE → EventSource GET /status/{task_id}
7. Cada worker hace BRPOP → agarra su tarea de la cola
8. Worker actualiza Redis → status = "en proceso"
9. SSE detecta el cambio → manda al frontend cada segundo
10. Frontend actualiza DOM → badge cambia a "en proceso"
11. Worker procesa CSV → calcula promedios/totales/inválidos
12. Worker actualiza Redis → status = "completada", result = datos
13. SSE manda resultado → frontend muestra los datos en la tarjeta
14. Los 3 terminan → SSE se cierra, aparece botón "Guardar"
15. Usuario guarda → localStorage guarda el resultado
```

