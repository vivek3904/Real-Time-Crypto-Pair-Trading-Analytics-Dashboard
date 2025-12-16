import asyncio
import websockets
import json
import redis.asyncio as redis
from typing import List, Dict

# --- Configuration ---
# You can use the specific trade stream for individual symbols,
# or a combined stream for better efficiency.
# We will subscribe to trade streams: <symbol>@trade
BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443/ws/"

# Define the symbols you want to track (make sure they are lowercase)
SYMBOLS_TO_TRACK = ["btcusdt", "ethusdt"]

# Redis connection details
REDIS_HOST = "localhost"
REDIS_PORT = 6379


class TickIngestor:
    """
    Handles the asynchronous connection to the Binance WebSocket and
    writes raw tick data directly into Redis Streams.
    """

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        # Initialize an asynchronous Redis client
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
        self.uri = self._build_websocket_uri()

    def _build_websocket_uri(self) -> str:
        """
        Builds the combined WebSocket URI for multiple trade streams.
        Example: wss://.../ws/btcusdt@trade/ethusdt@trade
        """
        streams = [f"{sym}@trade" for sym in self.symbols]
        stream_path = "/".join(streams)
        return f"{BINANCE_WS_BASE_URL}{stream_path}"

    async def connect_and_run(self):
        """
        Establishes the connection and enters the continuous listening loop.
        Includes a basic reconnection mechanism.
        """
        while True:
            try:
                print(f"Attempting to connect to Binance WS: {self.uri}")
                async with websockets.connect(self.uri) as websocket:
                    print("Connection established. Starting tick ingestion...")
                    await self.listen_for_ticks(websocket)
            except (websockets.ConnectionClosedOK, websockets.ConnectionClosedError, ConnectionRefusedError) as e:
                print(f"Connection closed/refused: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)

    async def listen_for_ticks(self, websocket):
        """
        The main loop for receiving and processing messages.
        """
        while True:
            try:
                # Receive the raw JSON message
                message = await websocket.recv()
                tick_data = json.loads(message)

                # Process and Store the tick
                await self.process_and_store(tick_data)

            except asyncio.TimeoutError:
                # Handle connection timeout if needed
                print("WebSocket receive timed out.")
                continue
            except Exception as e:
                # Break the loop on serious errors to trigger reconnection
                print(f"Error receiving/processing tick: {e}")
                break

    async def process_and_store(self, tick_data: Dict):
        """
        Extracts required fields and writes the tick to the Redis Stream.
        Required fields: timestamp, symbol, price, size/qty.
        Binance trade stream fields:
          's': Symbol, 'p': Price, 'q': Quantity/Size, 'E': Event time (timestamp)
        """
        # Ensure it's a trade message and not a subscription/status message
        if 's' in tick_data and 'p' in tick_data:
            symbol = tick_data['s']  # e.g., 'BTCUSDT'
            timestamp_ms = tick_data['E']
            price = tick_data['p']
            qty = tick_data['q']

            # Create the Redis Stream key (e.g., 'ticks:btcusdt')
            # Using lowercase is standard practice
            redis_stream_key = f"ticks:{symbol.lower()}"

            # The tick data dictionary to be stored in the stream.
            # All values in Redis Streams must be bytes/strings.
            processed_tick = {
                # Store timestamp (T), price (P), and quantity (Q)
                b'T': str(timestamp_ms).encode('utf-8'),
                b'P': price.encode('utf-8'),
                b'Q': qty.encode('utf-8'),
            }

            # XADD command writes the tick to the stream.
            # The '*' ensures Redis generates a unique ID for the message.
            await self.r.xadd(redis_stream_key, processed_tick)

            # Optional: Uncomment for live feedback
            # print(f"-> Stored {symbol} tick @ {price} ({datetime.fromtimestamp(timestamp_ms/1000):%H:%M:%S.%f})")


# --- Main execution block ---
async def main():
    """
    Initializes and runs the TickIngestor.
    """
    ingestor = TickIngestor(SYMBOLS_TO_TRACK)
    # This call is blocking and will run until manually stopped
    await ingestor.connect_and_run()


if __name__ == "__main__":
    # Note: Ensure a Redis server is running locally before executing this.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nIngestion stopped by user.")