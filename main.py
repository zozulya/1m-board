import asyncio
import time
from typing import Dict, Any

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel, conint
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/my_database"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    
    async with app.state.db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS board (
                row INT NOT NULL,
                col INT NOT NULL,
                color TEXT NOT NULL,
                PRIMARY KEY (row, col)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_last_update (
                user_id TEXT PRIMARY KEY,
                last_update BIGINT NOT NULL
            );
        """)
    
    app.state.notify_task = asyncio.create_task(listen_for_notifications(app.state.db_pool))
    
    yield
    
    # Cleanup
    app.state.notify_task.cancel()
    await app.state.db_pool.close()

app = FastAPI(title="Tile Board (Postgres Version)", lifespan=lifespan)


# We'll store all active WebSocket connections so we can broadcast NOTIFY messages.
active_websockets = set()

# Pydantic model for tile update
class TileUpdateRequest(BaseModel):
    row: conint(ge=0, le=999)
    col: conint(ge=0, le=999)
    color: str
    user_id: str

def current_timestamp() -> int:
    """Return the current timestamp in seconds."""
    return int(time.time())

async def listen_for_notifications(pool: asyncpg.Pool):
    """
    Background task: open a dedicated connection, LISTEN on 'tile_updates',
    and broadcast notifications to connected WebSockets.
    """
    try:
        # Acquire a dedicated connection for listening.
        async with pool.acquire() as conn:
            # Listen on channel
            await conn.execute("LISTEN tile_updates;")

            # Loop forever, waiting for NOTIFY messages.
            while True:
                # Wait for the next notification
                notification = await conn.connection.wait_for_notify()
                # notification.payload will be something like "row:col:color"
                payload = notification.payload

                # Broadcast to all active websockets
                dead_websockets = []
                for ws in active_websockets:
                    try:
                        await ws.send_text(payload)
                    except WebSocketDisconnect:
                        dead_websockets.append(ws)
                    except Exception as ex:
                        print(f"Error sending to WebSocket: {ex}")
                        dead_websockets.append(ws)

                # Remove any websockets that disconnected.
                for ws in dead_websockets:
                    active_websockets.remove(ws)

    except asyncio.CancelledError:
        # Task is canceled, exit gracefully
        pass


@app.get("/board")
async def get_board():
    """
    Return all rows from the board table.
    Warning: Could be 1 million records.
    """
    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT row, col, color FROM board ORDER BY row, col")
        # Return as list of dicts
        board_data = [
            {"row": r["row"], "col": r["col"], "color": r["color"]} for r in rows
        ]
    return board_data


@app.post("/update_tile")
async def update_tile(update_req: TileUpdateRequest):
    """
    Update a single tile if user hasn't updated in the last 5 minutes (300s).
    Then, NOTIFY other subscribers about the change.
    """
    now_ts = current_timestamp()
    print(f"Processing update request for user {update_req.user_id} at timestamp {now_ts}")
    
    async with app.state.db_pool.acquire() as conn:
        # 1. Check last update for user
        print(f"Checking last update time for user {update_req.user_id}")
        last_up_ts = await conn.fetchval(
            "SELECT last_update FROM user_last_update WHERE user_id=$1",
            update_req.user_id
        )

        if last_up_ts is not None and (now_ts - last_up_ts) < 300:
            print(f"Rate limit exceeded for user {update_req.user_id}. Last update: {last_up_ts}")
            raise HTTPException(
                status_code=429,
                detail="You can only update one tile every 5 minutes."
            )

        # 2. Update or insert the tile color
        print(f"Updating tile at position ({update_req.row}, {update_req.col}) to color {update_req.color}")
        await conn.execute(
            """
            INSERT INTO board (row, col, color)
            VALUES ($1, $2, $3)
            ON CONFLICT (row, col) DO UPDATE SET color = EXCLUDED.color
            """,
            update_req.row,
            update_req.col,
            update_req.color
        )

        # 3. Update the user's last update timestamp (upsert)
        print(f"Updating last update timestamp for user {update_req.user_id} to {now_ts}")
        await conn.execute(
            """
            INSERT INTO user_last_update (user_id, last_update)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET last_update = EXCLUDED.last_update
            """,
            update_req.user_id,
            now_ts
        )

        # 4. Use NOTIFY to broadcast the update to 'tile_updates' channel
        #    (payload is "row:col:color")
        payload = f"{update_req.row}:{update_req.col}:{update_req.color}"
        print(f"Broadcasting update notification with payload: {payload}")
        await conn.execute(f"NOTIFY tile_updates, '{payload}'")

    print("Update completed successfully")
    return {"message": "Tile updated successfully."}

@app.websocket("/ws/updates")
async def websocket_updates(websocket: WebSocket):
    """
    WebSocket that receives real-time tile updates via Postgres NOTIFY.
    We don't send messages to the database here; we only receive them.
    """
    await websocket.accept()
    active_websockets.add(websocket)

    try:
        while True:
            # This endpoint only *receives* notifications from Postgres via `listen_for_notifications`.
            # So we just wait for the client to disconnect or the server to shut down.
            data = await websocket.receive_text()
            # Usually you wouldn't expect data from the client, but let's just ignore it or handle pings.
            # If you want two-way communication, you could handle it here.
    except WebSocketDisconnect:
        # Client disconnected
        pass
    finally:
        active_websockets.discard(websocket)

# Initialize Prometheus Instrumentator
instrumentator = Instrumentator()

# Instrument all endpoints
instrumentator.instrument(app).expose(app)