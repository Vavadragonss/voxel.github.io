import asyncio
import json
import socket
import websockets
import os

# Server State
PLAYERS = {}  # websocket -> { username, x, y, z, rx, ry }
WORLD_MODIFICATIONS = {} # "x,y,z" -> block_id

def get_local_ip():
    """Dynamically fetches the machine's local network IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

async def broadcast(message, exclude=None):
    """Safely broadcasts messages. Stale or failing sockets won't crash the sender."""
    if not PLAYERS:
        return
    payload = json.dumps(message)
    
    async def safe_send(client, data):
        try:
            await client.send(data)
        except Exception:
            pass # Stale connections are auto-cleaned by handle_client's finally block

    tasks = [safe_send(client, payload) for client in list(PLAYERS.keys()) if client != exclude]
    if tasks:
        await asyncio.gather(*tasks)

async def handle_client(websocket):
    player_id = str(id(websocket))
    PLAYERS[websocket] = {"username": f"Player_{player_id[:4]}", "x":0, "y":0, "z":0, "rx":0, "ry":0}
    
    try:
        # Send initial world state to newly connected player
        await websocket.send(json.dumps({
            "type": "init",
            "id": player_id,
            "modifications": WORLD_MODIFICATIONS,
            "players": {str(id(ws)): data for ws, data in PLAYERS.items() if ws != websocket}
        }))
        
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "join":
                PLAYERS[websocket]["username"] = data["username"]
                await broadcast({
                    "type": "player_join",
                    "id": player_id,
                    "username": data["username"]
                }, exclude=websocket)
                
            elif msg_type == "move":
                PLAYERS[websocket].update({
                    "x": data["x"], "y": data["y"], "z": data["z"],
                    "rx": data["rx"], "ry": data["ry"]
                })
                await broadcast({
                    "type": "move",
                    "id": player_id,
                    "x": data["x"], "y": data["y"], "z": data["z"],
                    "rx": data["rx"], "ry": data["ry"]
                }, exclude=websocket)
                
            elif msg_type == "block_change":
                pos_key = f"{data['x']},{data['y']},{data['z']}"
                if data["block"] == 0:
                    WORLD_MODIFICATIONS.pop(pos_key, None)
                else:
                    WORLD_MODIFICATIONS[pos_key] = data["block"]
                await broadcast({
                    "type": "block_change",
                    "x": data["x"], "y": data["y"], "z": data["z"], "block": data["block"]
                }, exclude=websocket)
                
            elif msg_type == "chat":
                await broadcast({
                    "type": "chat",
                    "username": PLAYERS[websocket]["username"],
                    "message": data["message"]
                })
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if websocket in PLAYERS:
            username = PLAYERS[websocket]["username"]
            del PLAYERS[websocket]
            await broadcast({
                "type": "player_leave",
                "id": player_id,
                "username": username
            })

async def console_listener(server, default_path):
    """Asynchronously monitors command prompt input without stopping the engine loop."""
    print("Console listener active. Type 'Stop' to save changes and turn off server.")
    while True:
        command = await asyncio.to_thread(input)
        if command.strip().lower() == "stop":
            print("\n" + "="*40)
            print(" INITIATING SERVER SHUTDOWN SYSTEM")
            print("="*40)
            
            save_path = await asyncio.to_thread(input, f"Enter destination file path (Press Enter to default to '{default_path}'): ")
            save_path = save_path.strip() or default_path
            
            try:
                with open(save_path, 'w') as f:
                    json.dump(WORLD_MODIFICATIONS, f)
                print(f"Database Successfully Saved! Written to: {os.path.abspath(save_path)}")
            except Exception as e:
                print(f"Critical Error saving world configuration maps: {e}")
            
            print("Disconnecting game nodes...")
            server.close()
            await server.wait_closed()
            print("Server execution halted safely.")
            os._exit(0)

def select_or_create_world():
    """Initial console dialogue system logic."""
    global WORLD_MODIFICATIONS
    print("=" * 60)
    print("             VOXEL MULTIPLAYER SERVER MANAGER              ")
    print("=" * 60)
    print("[1] Create a completely new empty world map")
    print("[2] Load an existing world data file (.json)")
    
    choice = input("Select operation mode (1 or 2): ").strip()
    default_filename = "world.json"
    
    if choice == "2":
        path = input("Enter the relative or full path to the world file: ").strip()
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    WORLD_MODIFICATIONS.update(data)
                print(f"Success! Parsed and reconstructed {len(WORLD_MODIFICATIONS)} block modifications.")
                return path
            except Exception as e:
                print(f"Error loading target document: {e}. Defaulting to clean initialization.")
        else:
            print(f"File path '{path}' does not exist. Defaulting to clean initialization.")
            
    path_new = input(f"Set a default save file path for this world (Press Enter for '{default_filename}'): ").strip()
    return path_new if path_new else default_filename

async def main(active_world_path):
    # Listens on all available network interfaces at port 8080
    server = await websockets.serve(handle_client, "0.0.0.0", 8080)
    
    local_ip = get_local_ip()
    print("=" * 60)
    print(" VOXEL MULTIPLAYER SERVER IS LIVE!")
    print(f" -> Localhost connection string : ws://localhost:8080")
    print(f" -> Give this IP to your friends: {local_ip}:8080")
    print("=" * 60)
    
    # Run the console text scanner concurrently
    asyncio.create_task(console_listener(server, active_world_path))
    await server.wait_closed()

if __name__ == "__main__":
    # Handle user options through command line before opening asynchronous loop
    chosen_path = select_or_create_world()
    try:
        asyncio.run(main(chosen_path))
    except KeyboardInterrupt:
        print("\nProcess killed via terminal command.")