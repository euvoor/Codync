"""MCP fixture whose tools/list response waits for the test's explicit release."""
import json
import os
import socket
import sys

variant = sys.argv[1]
valid = os.environ.get("API_KEY") == "test-private-key"
valid = valid and not (os.environ.get("NEW_COMMAND") == "1" and variant == "changed")

for line in sys.stdin:
    message = json.loads(line)
    if "id" not in message:
        continue
    reply = {"jsonrpc": "2.0", "id": message["id"]}
    if not valid:
        reply["error"] = {"message": "incompatible fixture settings"}
    elif message["method"] == "initialize":
        reply["result"] = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "connector-edit-fixture", "version": "1"},
        }
    else:
        with socket.create_connection(("127.0.0.1", int(os.environ["EDIT_GATE_PORT"])), timeout=15) as gate:
            gate.sendall((variant + "\n").encode())
            if gate.recv(1) != b"x":
                raise RuntimeError("test did not release verification")
        reply["result"] = {"tools": []}
    print(json.dumps(reply), flush=True)
