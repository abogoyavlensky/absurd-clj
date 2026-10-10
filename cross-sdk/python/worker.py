"""Python side of the cross-SDK check: works the xsdk_py queue."""

from absurd_sdk import Absurd

app = Absurd(queue_name="xsdk_py")


@app.register_task("py-echo", default_max_attempts=1)
def py_echo(params, ctx):
    step = ctx.step("step", lambda: {"seen": params})
    ctx.sleep_for("nap", 1)
    go = ctx.await_event(f"go:{ctx.task_id}", timeout=60)
    return {"from": "python", "params": params, "step": step, "go": go}


print("python worker listening on xsdk_py", flush=True)
app.start_worker(worker_id="python-worker", claim_timeout=30, poll_interval=0.1)
