# camunda-l1-l3-demo

Standalone Spring Boot app that wires **L1 → L2 → L3** of the Java framework to a
real Zeebe broker for one tiny process: `rack_arrived → master_pptl_press`.
The `@JobWorker` (L3) is the entry point and holds L2 and L1 beneath it — this
module demos all three layers together, not L2 in isolation.

Built and smoke-tested in this session (`mvn clean install`, JDK 21 — the framework
targets Java 17; a bleeding-edge local JDK 26 install breaks Lombok, use 17 or 21).
The app boots, both `@JobWorker`s register, and the log below is the actual
run against no broker (connection refused is expected without Zeebe up):

```
Started DemoApplication in 1.585 seconds
... Failed to activate jobs for worker rackArrivedWorker#handle ... Connection refused: localhost:26500
```

Point it at your running local broker and that line changes to real job activity.

## What's wired to what

| Node in the BPMN | L3 worker | L2 saga | L1 |
|---|---|---|---|
| `Task_RackArrived` (type `pick.demo.rack_arrived`) | `RackArrivedWorker` | `RackArrivedSaga` — rule: `workOrderId` present | `RackEntity` (id=workOrderId, status DOCKED) via `RackRepository` |
| `Task_MasterPptlPress` (type `pick.demo.master_pptl_press`) | `MasterPptlPressWorker` | `MasterPptlPressSaga` — rule: rack must already be DOCKED | same `RackEntity`, status → BIN_LIT |

Both sagas go through the exact `runSaga()` contract from `camunda-l2-starter`:
rule check (before any write) → commit (inside `@Transactional`) → checkpoint
(`WorkflowCheckpoint`, step = the BPMN task id) → publish (`DemoLoggingMq`,
fires only after commit). `WorkflowContext` is set/cleared around each job so
the console log lines carry `[wf=...]` from the process's `workflow_id` variable.

## Run it

1. **Start your local Zeebe broker / Camunda 8 Run / Operate** the way you normally do.
2. **Deploy the BPMN** — open `src/main/resources/bpmn/pick_demo_rack_pptl.bpmn` in
   Camunda Desktop Modeler and hit Deploy (point it at your broker's REST/gRPC address).
3. **Build and run the app** (from the `java_camunda_framework` repo root):
   ```bash
   mvn -pl camunda-l1-l3-demo -am clean install -DskipTests
   java -jar camunda-l1-l3-demo/target/camunda-l1-l3-demo.jar
   ```
   If your broker isn't on the default `localhost:26500`, set
   `ZEEBE_GATEWAY_ADDRESS=host:port` first.
4. **Start a process instance** — via Modeler's Run/Play button, Operate, or `zbctl`,
   with starting variables:
   ```json
   { "work_order_id": "WO-1001", "workflow_id": "WO-1001", "tenant_id": "demo" }
   ```
5. **Watch it in Operate** — the instance moves through both service tasks; check
   the variables panel for `rack_status` → `DOCKED` then `bin_light` → `ON`. The
   app's console shows the MDC-tagged log lines and the `MQ PUBLISH` line for
   each saga.

## Proving "no restart" live

With the app still running, edit the BPMN in Modeler (e.g. rename a task label),
redeploy — **don't touch the running app** — then start a new instance. The same
JVM, same `@JobWorker` beans, picks up jobs against the new process definition
version immediately, because Zeebe workers poll by job **type string**, not by
process version. That's the whole "hot redeploy" story in one click.

## Known limitation

`camunda-l4-starter` (screen template push) and an L5 client aren't part of this
demo — see the L4/L5 slide. This app only exercises L1-L3.
