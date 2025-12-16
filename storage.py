import pandas as pd
import duckdb
import redis
import time
import threading
from typing import List, Dict, Optional

# --- Configuration ---
DUCKDB_FILE = "quant_data.db"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_GROUP = "resample_group"  # Consumer group name for Redis Streams
REDIS_CONSUMER = "resample_worker_1"  # Unique consumer name

# Timeframes required for resampling
RESAMPLE_TIMEFRAMES = ['1s', '1min', '5min']


class DataResampler:
    """
    Background worker that reads raw ticks from Redis Streams,
    resamples them into OHLCV bars, and stores them in DuckDB.
    """

    def __init__(self, symbols: List[str]):
        """
        Initializes connections and creates necessary structures.
        """
        self.symbols = [s.lower() for s in symbols]

        # Initialize synchronous Redis client for streaming operations
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

        # Initialize DuckDB connection
        self.db = duckdb.connect(database=DUCKDB_FILE)

        # Create OHLCV table in DuckDB if it doesn't exist
        self._setup_duckdb_table()

        # Create Redis Consumer Groups
        self._setup_redis_groups()

        # Dictionary to temporarily hold tick data for resampling
        # Key: symbol, Value: list of raw ticks
        self.tick_buffer: Dict[str, List[Dict]] = {sym: [] for sym in self.symbols}

        self._stop_event = threading.Event()

    def _setup_duckdb_table(self):
        """Creates the persistent OHLCV table in DuckDB."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol      VARCHAR,
                time        TIMESTAMP,
                open        DOUBLE,
                high        DOUBLE,
                low         DOUBLE,
                close       DOUBLE,
                volume      DOUBLE,
                timeframe   VARCHAR,
                PRIMARY KEY (symbol, time, timeframe) -- Ensures no duplicates
            );
        """)
        print(f"DuckDB table 'ohlcv' initialized at {DUCKDB_FILE}")

    def _setup_redis_groups(self):
        """Creates a Redis consumer group for each symbol stream."""
        for sym in self.symbols:
            stream_key = f"ticks:{sym}"
            try:
                # Create the stream key if it doesn't exist (MKSTREAM)
                # $ means start reading from the last entry
                self.r.xgroup_create(stream_key, REDIS_GROUP, id='0', mkstream=True)
                print(f"Redis Consumer Group '{REDIS_GROUP}' created for stream '{stream_key}'.")
            except redis.exceptions.ResponseError as e:
                # Usually occurs if the group already exists
                if "BUSYGROUP" not in str(e):
                    raise e
                # print(f"Redis Consumer Group for {sym} already exists.")

    def _fetch_and_buffer_ticks(self):
        """Reads new ticks from all Redis Streams using the consumer group."""

        # Create a dictionary of streams to read from
        streams_to_read = {f"ticks:{sym}": '>' for sym in self.symbols}

        try:
            # XREADGROUP command: read new messages for the consumer group
            # 'count' limits the number of messages per stream read at once
            response = self.r.xreadgroup(
                REDIS_GROUP,
                REDIS_CONSUMER,
                streams_to_read,
                count=500,
                block=500  # Block for 500ms if no new data
            )

            # Process the response structure: [[stream_key, [[msg_id, fields], ...]], ...]
            for stream_key, messages in response:
                symbol = stream_key.decode('utf-8').split(':')[-1]

                # A list to store the IDs of messages successfully processed
                # for later Acknowledge (ACK)
                message_ids = []

                for message_id, fields in messages:
                    try:
                        # Extract and decode the tick fields
                        tick = {
                            'T': int(fields[b'T']),  # Timestamp in ms
                            'P': float(fields[b'P']),  # Price
                            'Q': float(fields[b'Q'])  # Quantity
                        }
                        self.tick_buffer[symbol].append(tick)
                        message_ids.append(message_id)

                    except Exception as e:
                        print(f"Error decoding tick from Redis: {e}")

                # Acknowledge the successfully processed messages
                if message_ids:
                    self.r.xack(stream_key, REDIS_GROUP, *message_ids)
                    # print(f"ACKed {len(message_ids)} messages for {symbol}")

        except Exception as e:
            print(f"Error during Redis XREADGROUP operation: {e}")
            time.sleep(1)

    def _process_and_resample(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Takes buffered ticks for one symbol, resamples them, and clears the buffer.
        """
        if not self.tick_buffer[symbol]:
            return None

        # 1. Convert buffered list of dicts to Pandas DataFrame
        df = pd.DataFrame(self.tick_buffer[symbol])

        # Clear the buffer immediately after copying the data
        self.tick_buffer[symbol] = []

        # 2. Prepare the DataFrame for resampling
        df['time'] = pd.to_datetime(df['T'], unit='ms')
        df = df.set_index('time').sort_index()

        # 3. Perform resampling for all defined timeframes
        final_ohlcv_data = []
        for tf in RESAMPLE_TIMEFRAMES:
            # Resample Price (P) to OHLC
            ohlc_df = df['P'].resample(tf).ohlc()
            # Resample Quantity (Q) to Volume (sum)
            volume_df = df['Q'].resample(tf).sum().rename('volume')

            # Combine OHLC and Volume
            resampled_df = ohlc_df.join(volume_df).dropna()

            if not resampled_df.empty:
                # Add metadata columns
                resampled_df['symbol'] = symbol.lower()
                resampled_df['timeframe'] = tf

                # Reset index to move 'time' back to a column
                resampled_df = resampled_df.reset_index()
                resampled_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'timeframe']

                # Reorder the columns to match the DuckDB table definition (if necessary, though not strictly required if named correctly)
                resampled_df = resampled_df[['symbol', 'time', 'open', 'high', 'low', 'close', 'volume', 'timeframe']]
                # Add to the list for bulk insertion
                final_ohlcv_data.append(resampled_df)

        if final_ohlcv_data:
            # Concatenate all resampled data across all timeframes
            return pd.concat(final_ohlcv_data)
        return None

    def _store_to_duckdb(self, df: pd.DataFrame):
        """Inserts the processed DataFrame into DuckDB."""
        try:
            # DuckDB allows direct insertion from a Pandas DataFrame
            # This uses the ON CONFLICT clause via the temporary table structure
            self.db.execute("INSERT OR REPLACE INTO ohlcv SELECT * FROM df")
            # print(f"Successfully stored {len(df)} OHLCV bars in DuckDB.")
        except Exception as e:
            print(f"Error inserting into DuckDB: {e}")

    def run_worker_thread(self):
        """The main loop for the background worker thread."""
        print("Data Resampler worker started...")
        while not self._stop_event.is_set():
            # Step 1: Fetch and buffer new ticks from Redis
            self._fetch_and_buffer_ticks()

            # Step 2: Process and resample buffered data for each symbol
            for symbol in self.symbols:
                resampled_data = self._process_and_resample(symbol)

                # Step 3: Store the final OHLCV bars into DuckDB
                if resampled_data is not None and not resampled_data.empty:
                    self._store_to_duckdb(resampled_data)

            # Sleep briefly to avoid busy-waiting, but rely mostly on the Redis block timeout
            time.sleep(0.1)

        print("Data Resampler worker stopped.")

    def start(self):
        """Starts the worker thread."""
        self.thread = threading.Thread(target=self.run_worker_thread)
        self.thread.daemon = True  # Allows the main program to exit even if this thread is running
        self.thread.start()

    def stop(self):
        """Stops the worker thread."""
        self._stop_event.set()
        if self.thread.is_alive():
            self.thread.join()

# --- Example Usage (for testing or integration into a main script) ---
# if __name__ == "__main__":
#     SYMBOLS = ["btcusdt", "ethusdt"] # Match symbols in ingestion.py
#     resampler = DataResampler(SYMBOLS)

#     # Ensure you have data flowing from ingestion.py before running this
#     resampler.start()
#
#     try:
#         print("Resampler running. Press Ctrl+C to stop...")
#         while True:
#             time.sleep(5)
#             # Example: Query the database every 5 seconds
#             print("\n--- Current 1m Data Snapshot ---")
#             print(resampler.db.execute("SELECT * FROM ohlcv WHERE timeframe='1m' ORDER BY time DESC LIMIT 2;").fetchdf())
#
#     except KeyboardInterrupt:
#         print("Shutting down...")
#         resampler.stop()