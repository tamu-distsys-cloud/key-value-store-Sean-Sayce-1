#ChatGPT was used for debugging my Get and Append fucntions, as well as commenting

# Import standard libraries and RPC types
import random
import threading
from typing import List
from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

# Generate a unique 62-bit integer
def nrand() -> int:
    return random.getrandbits(62)

# Compute the shard index for a key
def safe_shard(key: str, nshards: int) -> int:
    return sum(ord(c) for c in key) % nshards

class Clerk:
    # Initialize the Clerk with server list and config
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        # Assign a random client ID
        self.client_id = nrand()
        # Start request ID at zero
        self.request_id = 0
        # Use lock for thread-safe ID increment
        self.lock = threading.Lock()

    # Safely generate a new request ID
    def next_request_id(self) -> int:
        with self.lock:
            self.request_id += 1
            return self.request_id

    # Handle Get RPC from the client
    def get(self, key: str) -> str:
        # Create request args with unique ID
        request_id = self.next_request_id()
        args = GetArgs(key, self.client_id, request_id)

        # Get total shards and replicas
        nshards = self.cfg.nservers
        nreplicas = self.cfg.nreplicas
        # Determine shard and its primary
        shard = safe_shard(key, nshards)
        primary = shard % nshards
        # Generate list of replica server IDs
        group = [(primary + i) % nshards for i in range(nreplicas)]

        # Set retry attempt counters
        attempts = 0
        max_attempts = 10000

        # Retry loop for the Get request
        while attempts < max_attempts:
            for sid in group:
                try:
                    # Attempt to call the server
                    reply: GetReply = self.servers[sid].call("KVServer.Get", args)
                    # Return value if successful
                    if reply and reply.err == "":
                        return reply.value
                # Ignore timeouts or failures
                except Exception:
                    continue
            # Increment retry counter
            attempts += 1
        # Return empty if all attempts fail
        return ""

    # Shared logic for Put and Append operations
    def put_append(self, key: str, value: str, op: str) -> str:
        # Create request args with unique ID
        request_id = self.next_request_id()
        args = PutAppendArgs(key, value, self.client_id, request_id)

        # Get shard and replica server list
        nshards = self.cfg.nservers
        nreplicas = self.cfg.nreplicas
        shard = safe_shard(key, nshards)
        primary = shard % nshards
        replicas = [(primary + i) % nshards for i in range(nreplicas)]

        # Set retry attempt counters
        attempts = 0
        max_attempts = 10000

        # Retry loop for Put/Append RPC
        while attempts < max_attempts:
            for sid in replicas:
                try:
                    # Attempt to call the server
                    reply: PutAppendReply = self.servers[sid].call(f"KVServer.{op}", args)
                    # Return old value (for append) or empty
                    if reply and reply.err == "":
                        return getattr(reply, "prev_value", "")
                    # Retry on shard errors
                    elif reply and reply.err in ("ErrWrongServer", "ErrNoPrimaryAvailable"):
                        continue
                # Ignore timeout or call failure
                except Exception:
                    continue
            # Increment retry counter
            attempts += 1
        # Return empty if all attempts fail
        return ""

    # Public Put interface
    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Public Append interface
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
