#!/usr/bin/env python3
"""
Mock butler_server for the pick_vanilla_demo Camunda demo.

Implements the same 11 routes as pick_vanilla_demo_http_handler.erl / _controller.erl,
with the exact response shapes ButlerServerApiClient.java expects. Point
BUTLER_SERVER_BASE_URL at this instead of a real butler_server to run the full
Camunda flow end-to-end without touching any real VM/Mnesia.

Every response is edited in one place below (MOCK_DATA / DECISIONS) -- change
values there to steer which BPMN branches the demo takes.

Run:
    python3 mock_server.py [port]      # default port 8081

To demo a failed API call / retry, inject a failure on a specific route:
    python3 mock_server.py 8081 --fail-endpoint hasClearableFrontBin --fail-count 2
        # first 2 calls to .../hasClearableFrontBin return 500, then it succeeds again --
        # the BPMN's retries="3" means Zeebe auto-retries and this recovers with no incident.
    python3 mock_server.py 8081 --fail-endpoint hasClearableFrontBin --fail-count -1
        # fails forever -- retries exhaust and Zeebe raises a permanent incident on that task.
--fail-endpoint takes the route's last path segment (hasClearableFrontBin, isPickPossible,
ppsBinDetails, etc.) and --fail-status (default 500) sets the HTTP status returned.

To demo the inner/outer loops actually looping a controlled number of times before
terminating (rather than never looping, or looping forever):
    python3 mock_server.py 8081 --more-instructions-count 2
        # outer loop: any_more_pick_instructions returns true for the next 2 calls
        # (looping back to Task_HasClearableFrontBin each time), then false -- the
        # rack proceeds to auto_dest_clear/rack_released on the 3rd pass.
    python3 mock_server.py 8081 --more-bins-count 3
        # inner loop: any_more_bins_in_batch returns true for the next 3 calls
        # (looping back to Event_WaitForPickBinConfirm each time), then false.
    python3 mock_server.py 8081 --more-instructions-count -1
        # -1 = loops forever, exactly like --fail-count -1 -- useful for watching the
        # loop run live in Operate, kill the app when you've seen enough.
Both can be combined and both default to 0 (no looping), so the existing single-pass
demo is unaffected unless you explicitly ask for a loop.
"""
import argparse
import itertools
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

_parser = argparse.ArgumentParser()
_parser.add_argument("port", nargs="?", type=int, default=8081)
_parser.add_argument("--fail-endpoint", default=None, help="Route name to fail, e.g. hasClearableFrontBin")
_parser.add_argument("--fail-count", type=int, default=0, help="Calls to fail before recovering; -1 = fail forever")
_parser.add_argument("--fail-status", type=int, default=500, help="HTTP status code to return while failing")
_parser.add_argument("--more-instructions-count", type=int, default=0,
                      help="Outer-loop: any_more_pick_instructions returns true this many times "
                           "before returning false; -1 = loop forever")
_parser.add_argument("--more-bins-count", type=int, default=0,
                      help="Inner-loop: any_more_bins_in_batch returns true this many times "
                           "before returning false; -1 = loop forever")
_args = _parser.parse_args()

PORT = _args.port
FAILURE_STATE = {
    "endpoint": _args.fail_endpoint,
    "remaining": float("inf") if _args.fail_count < 0 else _args.fail_count,
    "status": _args.fail_status,
}

# Each loop gets its own remaining-count and its own iteration counter (the latter only
# used to make the mock pick-instruction/bin data below look distinct per loop pass).
LOOP_STATE = {
    "more_instructions": {
        "remaining": float("inf") if _args.more_instructions_count < 0 else _args.more_instructions_count,
        "iteration": itertools.count(1),
    },
    "more_bins": {
        "remaining": float("inf") if _args.more_bins_count < 0 else _args.more_bins_count,
        "iteration": itertools.count(1),
    },
}


def maybe_fail(endpoint_name, handler):
    """Injects the configured failure for one route; returns True if it handled the response."""
    if FAILURE_STATE["endpoint"] == endpoint_name and FAILURE_STATE["remaining"] > 0:
        FAILURE_STATE["remaining"] -= 1
        print(f"[mock-butler-server]   INJECTED FAILURE on {endpoint_name} "
              f"(status={FAILURE_STATE['status']}, remaining={FAILURE_STATE['remaining']})")
        handler._send_json({"error": f"mock injected failure on {endpoint_name}"}, status=FAILURE_STATE["status"])
        return True
    return False


def consume_loop(name):
    """
    Returns (should_loop, iteration) for the named loop ("more_instructions" or
    "more_bins"), decrementing its remaining count. Mirrors maybe_fail's counter
    pattern, just returning a boolean result instead of injecting an HTTP failure.
    """
    state = LOOP_STATE[name]
    if state["remaining"] > 0:
        state["remaining"] -= 1
        iteration = next(state["iteration"])
        print(f"[mock-butler-server]   {name}: looping (iteration={iteration}, remaining={state['remaining']})")
        return True, iteration
    return False, None

# ---------------------------------------------------------------------------
# Edit these to steer the demo. All booleans below are read by the BPMN's
# exclusive gateways (Gateway_A..F) -- see pick_vanilla_demo.bpmn.
# ---------------------------------------------------------------------------
DECISIONS = {
    "has_clearable_front_bin": False,          # Gateway_A: tote-flow branch if true
    "is_front_tote_flow": False,                # Gateway_A
    "is_destination_orchestrated_by_htm": False,  # Gateway_B
    "is_pick_possible": True,                    # Gateway_C -- MUST be true to reach the rest of the flow
    "print_awaited": False,                      # Gateway_E
    # any_more_pick_instructions / any_more_bins_in_batch are NOT here -- they're
    # controlled by --more-instructions-count / --more-bins-count instead (see the
    # module docstring), since a plain True/False can't express "loop N times then
    # stop" the way a countdown can.
}

MOCK_DATA = {
    "rack": {
        "rack_id": "MOCK-RACK-001",
        "status": "at_pps",
        "face": "F",
    },
    "pps_bin": {
        "pps_id": "1",
        "bin_id": "front_1",
        "status": "empty",
        "item_count": 0,
    },
    "pps": {
        "pps_id": "1",
        "status": "open",
        "mode": "pick",
    },
    "config": {
        "skip_carrier_wait_before_pick_start": "false",
        "enable_inventory_count_check": "false",
        "pick_empty_confirmation_required": "false",
        "release_rack_on_last_scan": "true",
        "streaming_orders": "false",
    },
}


def found_response(data):
    return {"found": True, "data": json.dumps(data)}


class MockHandler(BaseHTTPRequestHandler):
    def _send_json(self, body, status=200):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        print(f"[mock-butler-server] {self.command} {self.path}")

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        def q(name, default=None):
            return qs.get(name, [default])[0]

        if maybe_fail(path.rstrip("/").rsplit("/", 1)[-1], self):
            return

        if path.endswith("/hasClearableFrontBin"):
            self._send_json({"result": DECISIONS["has_clearable_front_bin"]})
        elif path.endswith("/isFrontToteFlow"):
            self._send_json({"result": DECISIONS["is_front_tote_flow"]})
        elif path.endswith("/isDestinationOrchestratedByHtm"):
            self._send_json({"result": DECISIONS["is_destination_orchestrated_by_htm"]})
        elif path.endswith("/printAwaited"):
            ids = ["MOCK-DOCK-1"] if DECISIONS["print_awaited"] else []
            self._send_json({"dock_station_ids": ids})
        elif path.endswith("/anyMorePickInstructions"):
            more, iteration = consume_loop("more_instructions")
            body = {"result": more}
            if more:
                # Illustrative only -- ButlerServerApiClient/the Java operation only
                # reads "result"; these extra fields exist so the mock's own log/
                # response is readable while watching the outer loop actually run.
                body["next_pick_instruction"] = {
                    "pick_instruction_id": f"MOCK-PI-{iteration:03d}",
                    "bin_type": "bin",
                    "qty": 1,
                }
            self._send_json(body)
        elif path.endswith("/anyMoreBinsInBatch"):
            more, iteration = consume_loop("more_bins")
            body = {"result": more}
            if more:
                body["next_bin"] = {
                    "bin_id": f"MOCK-BIN-{iteration:03d}",
                    "bin_side": "F",
                }
            self._send_json(body)
        elif path.endswith("/rackDetails"):
            rack_id = q("rackId", "unknown")
            data = dict(MOCK_DATA["rack"], rack_id=rack_id)
            self._send_json(found_response(data))
        elif path.endswith("/ppsBinDetails"):
            pps_id = q("ppsId", "unknown")
            bin_id = q("binId", "unknown")
            data = dict(MOCK_DATA["pps_bin"], pps_id=pps_id, bin_id=bin_id)
            self._send_json(found_response(data))
        elif path.endswith("/ppsDetails"):
            pps_id = q("ppsId", "unknown")
            data = dict(MOCK_DATA["pps"], pps_id=pps_id)
            self._send_json(found_response(data))
        elif path.endswith("/config"):
            key = q("key", "")
            value = MOCK_DATA["config"].get(key)
            if value is None:
                self._send_json({"value": "<not allow-listed>"})
            else:
                self._send_json({"value": value})
        else:
            self._send_json({"error": "no such mock route", "path": path}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if maybe_fail(parsed.path.rstrip("/").rsplit("/", 1)[-1], self):
            return

        if parsed.path.endswith("/isPickPossible"):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {}
            print(f"[mock-butler-server]   isPickPossible body: {body}")
            self._send_json({"result": DECISIONS["is_pick_possible"]})
        else:
            self._send_json({"error": "no such mock route", "path": parsed.path}, status=404)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), MockHandler)
    print(f"[mock-butler-server] listening on http://localhost:{PORT}")
    print(f"[mock-butler-server] decisions: {DECISIONS}")
    if FAILURE_STATE["endpoint"]:
        print(f"[mock-butler-server] failure injection armed: {FAILURE_STATE}")
    if LOOP_STATE["more_instructions"]["remaining"] > 0:
        print(f"[mock-butler-server] outer loop armed: more_instructions_remaining={LOOP_STATE['more_instructions']['remaining']}")
    if LOOP_STATE["more_bins"]["remaining"] > 0:
        print(f"[mock-butler-server] inner loop armed: more_bins_remaining={LOOP_STATE['more_bins']['remaining']}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[mock-butler-server] shutting down")
        server.server_close()
