"""Python side of the reverse check: spawns a task the Clojure worker runs."""

import sys

from absurd_sdk import Absurd

app = Absurd(queue_name="xsdk_clj")

spawned = app.spawn("clj-echo", {"n": 1, "who": "python"}, queue="xsdk_clj")
# Events are cached first-emit-wins, so emitting before the task waits is safe.
app.emit_event(f"go:{spawned['task_id']}", {"ok": True})

try:
    result = app.await_task_result(spawned["task_id"], timeout=90)
    ok = result.state == "completed" and (result.result or {}).get("from") == "clojure"
    print("python->clojure:", result.state, result.result, flush=True)
except Exception as err:  # noqa: BLE001 - report anything, then signal done
    ok = False
    print("python->clojure: error", repr(err), flush=True)

app.emit_event("xsdk:done", {"ok": ok})
sys.exit(0 if ok else 1)
