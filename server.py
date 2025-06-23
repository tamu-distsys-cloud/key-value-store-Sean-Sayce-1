#ChatGPT was used for debugging the KServer class, as well as for commenting

# Import logging, threading, and typing support
import logging
import threading
from typing import Any

# Enable or disable debug output
debugging = False

# Format debug messages if enabled
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# RPC arguments for Put/Append requests
class PutAppendArgs:
    def __init__(self, key, value, client_id, request_id):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.request_id = request_id

# RPC reply for Put/Append responses
class PutAppendReply:
    def __init__(self, value):
        self.prev_value = value
        self.err = ""

# RPC arguments for Get requests
class GetArgs:
    def __init__(self, key, client_id, request_id):
        self.key = key
        self.client_id = client_id
        self.request_id = request_id

# RPC reply for Get responses
class GetReply:
    def __init__(self, value):
        self.value = value
        self.err = ""

# The core key-value server class
class KVServer:
    # Initialize the server state and config
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.store = {}
        self.client_req_cache = {}
        self.nservers = cfg.nservers
        self.nshards = cfg.nservers
        self.nreplicas = cfg.nreplicas

    # Return the server's index in cfg.kvservers
    def my_id(self):
        for i, s in enumerate(self.cfg.kvservers):
            if s is self:
                return i
        return -1

    # Compute the shard number for a key
    def shard_for(self, key):
        return sum(ord(c) for c in key) % self.nshards

    # Check if this server is primary for a key
    def is_primary_for(self, key):
        shard = self.shard_for(key)
        return self.my_id() == shard % self.nservers

    # Detect if this request is a duplicate
    def is_duplicate(self, client_id, request_id):
        cached = self.client_req_cache.get(client_id)
        if cached and request_id <= cached[0]:
            return True, cached[1]
        return False, None

    # Cache the latest reply for a client
    def update_cache(self, client_id, request_id, reply):
        self.client_req_cache[client_id] = (request_id, reply)

    # Compute replica group responsible for a key
    def _replica_group_for(self, key):
        nshards = self.cfg.nservers
        nreplicas = self.cfg.nreplicas
        shard = self.shard_for(key)
        primary = shard % nshards
        return [(primary + i) % nshards for i in range(nreplicas)]

    # Check if this server is in the replica group
    def _responsible_for(self, key) -> bool:
        return self.my_id() in self._replica_group_for(key)

    # Replicate the key-value pair to backups
    def _replicate_to_backups(self, key, value):
        group = self._replica_group_for(key)[1:]  # Skip self (primary)
        for sid in group:
            replica = self.cfg.kvservers[sid]
            # Acquire replica lock before writing
            with replica.mu:
                replica.store[key] = value

    # Handle Get RPC request
    def Get(self, args: GetArgs):
        # Reject if not responsible for key
        if not self._responsible_for(args.key):
            reply = GetReply("")
            reply.err = "ErrWrongServer"
            return reply

        # Read the key's value under lock
        with self.mu:
            value = self.store.get(args.key, "")
            reply = GetReply(value)
            reply.err = ""
            return reply

    # Handle Put RPC request
    def Put(self, args: PutAppendArgs):
        # Reject if not the primary
        if not self.is_primary_for(args.key):
            r = PutAppendReply("")
            r.err = "ErrWrongServer"
            return r

        # Acquire lock and check for duplicates
        with self.mu:
            dup, r_cached = self.is_duplicate(args.client_id, args.request_id)
            if dup:
                return r_cached
            # Store new value and update cache
            self.store[args.key] = args.value
            r = PutAppendReply("")
            self.update_cache(args.client_id, args.request_id, r)

        # Replicate to backup servers
        self._replicate_to_backups(args.key, args.value)
        return r

    # Handle Append RPC request
    def Append(self, args: PutAppendArgs):
        # Reject if not the primary
        if not self.is_primary_for(args.key):
            r = PutAppendReply("")
            r.err = "ErrWrongServer"
            return r

        # Acquire lock and check for duplicates
        with self.mu:
            dup, r_cached = self.is_duplicate(args.client_id, args.request_id)
            if dup:
                return r_cached
            # Get old value and append new data
            old = self.store.get(args.key, "")
            new = old + args.value
            self.store[args.key] = new
            r = PutAppendReply(old)
            self.update_cache(args.client_id, args.request_id, r)

        # Replicate updated value to backups
        self._replicate_to_backups(args.key, new)
        return r
