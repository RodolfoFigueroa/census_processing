import asyncio
import json

import geopandas as gpd
import requests
import websockets

WS_URL = "ws://localhost:5219/ws/analyze"
HTTP_BASE_URL = "http://localhost:5219"

df = gpd.read_file(
    r"C:\Users\lain\OneDrive - Instituto Tecnologico y de Estudios Superiores de Monterrey\mexicali_data\generated\agebs.gpkg"
)
test = json.loads(df[["geometry"]].reset_index(names="cvegeo").to_json())


async def run_analysis_pipeline(geojson: dict, function_name: str):
    """
    Submits a job via WebSocket, waits for the notification,
    and downloads the heavy result via HTTP.
    """
    print(f"1. Connecting to WebSocket at {WS_URL}...")

    try:
        async with websockets.connect(WS_URL) as websocket:
            # --- STEP 1: MAKE THE REQUEST ---
            request_payload = {"function_name": function_name, "geojson": geojson}
            print(f"   -> Sending request to run: {function_name}")
            await websocket.send(json.dumps(request_payload))

            # Wait for the immediate server acknowledgment
            ack_str = await websocket.recv()
            ack = json.loads(ack_str)
            print(f"   -> Server acknowledged. Task ID: {ack.get('task_id')}")

            # --- STEP 2: PROCESS THE NOTIFICATION ---
            print("\n2. Waiting for Celery worker to finish...")
            # This blocks efficiently without polling until the worker publishes to Redis
            notification_str = await websocket.recv()
            notification = json.loads(notification_str)

            if notification.get("status") == "error":
                print(f"\n❌ Worker failed: {notification.get('message')}")
                return None

            if notification.get("status") == "success":
                download_id = notification.get("download_id")
                print(f"\n✅ Worker finished! Received download ticket: {download_id}")

                # --- STEP 3: GET THE RESULTS ---
                print("3. Downloading massive payload via HTTP...")
                download_url = f"{HTTP_BASE_URL}/download-result/{download_id}"

                # Use standard requests to fetch the heavy JSON
                response = requests.get(download_url)

                if response.status_code == 200:
                    final_data = response.json()
                    print("   -> Download complete!")
                    return final_data
                print(f"❌ Failed to download data. HTTP {response.status_code}")
                print(response.text)
                return None

    except websockets.exceptions.ConnectionClosedError:
        print("\n❌ Error: The WebSocket connection was closed unexpectedly.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")


# --- Execute the Pipeline ---
if __name__ == "__main__":
    # Example bounding box coordinates
    my_polygon = [
        [-120.0, 35.0],
        [-119.0, 35.0],
        [-119.0, 36.0],
        [-120.0, 36.0],
        [-120.0, 35.0],
    ]

    # Run the async pipeline
    # (Assuming you have a function named 'ndvi_analysis' registered via your dynamic loader)
    final_result = res = asyncio.run(run_analysis_pipeline(test, "tree_coverage"))

    if final_result:
        print("\n--- Final Data Preview ---")
        # Print just a snippet of the data to prove it worked without flooding the terminal
        preview = str(final_result)[:200] + " ... [data truncated]"
        print(preview)
