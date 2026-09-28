# camunda-pick-vanilla-migration

**Camunda 8 orchestrates the pick-front vanilla flow. butler_server drives
nothing.** This replaces butler_server's `pick_front_hsm` for the vanilla
(`bin`-agent) flow. Every real HSM node — named after its actual Erlang
module — is a Java class + BPMN element, in the same order and with the
same branching the real HSM uses: **real exclusive gateways route where the
HSM really branches**, not a hidden decision inside a service task.

The cutover is config-gated per PPS
(`pick_camunda_orchestration_config:enabled_for_pps/1`, default `false`) —
a PPS not in the allowlist behaves exactly as it does today.

## Architecture

```
butler_server (VM)                                    this app (local)
────────────────────                                   ─────────────────
pick_entity_transport_event_handler.erl        ──Kafka──▶ WaitForSourceArrivalNode (START)
  (rack arrival, flag on)   topic: pick.camunda_migration.add_pick_list

butler_pps_api_handler.erl                     ──Kafka──▶ WaitForPickBinConfirmNode
  (PPTL press, flag on)     topic: pick.camunda_migration.pptl_press
                                                ──Kafka──▶ WaitForEntityScanNode
  (scan/multiscan, flag on) topic: pick.camunda_migration.scan

pick_camunda_orchestration_http_handler.erl    ◀──REST── ButlerServerApiClient
  GET  hasClearableFrontBin, isFrontToteFlow,           (called from the
       isDestinationOrchestratedByHtm, printAwaited      6 gateway-decision
  POST isPickPossible, clearBin, releaseRack,            nodes + 3 write nodes)
       commitPptlPress (STUBBED — see below)
```

## The full node map, with real gateways

35 BPMN elements: 1 start, 25 tasks/catch-events (one per real HSM module),
6 exclusive gateways, 3 end events. **Java only computes** (calls the read
API, sets the result as a process variable) — **the BPMN gateway routes**,
reading that variable with a FEEL condition. This is the opposite of an
earlier build in this session where the job worker itself decided the
branch and the diagram stayed a straight line; that didn't actually look
like — or behave like — the real HSM's branching shape.

| Real HSM module | Node kind | Gateway this precedes |
|---|---|---|
| `wait_for_source_arrival_node` | Message start (real Kafka) | — |
| `waiting_for_carrier_validation_node` | Pass-through | — |
| `wait_for_work_allocation_node` | Pass-through | — |
| `wait_for_work_allocation_vanilla_pps_node` | Gateway decision (`hasClearableFrontBin`, `isFrontToteFlow`) | **Gateway A** → `wait_for_dest_clear_node` / `wait_for_tote_attachment_node` |
| `wait_for_dest_clear_node` | Placeholder (see below) | — |
| `wait_for_tote_attachment_node` | Placeholder | — |
| `pick_pre_requisites_node` | Gateway decision (`isDestinationOrchestratedByHtm`) | **Gateway B** → `waiting_for_dest_agent_node` / skip |
| `waiting_for_dest_agent_node` | Pass-through | — |
| `picklist_processing_node` | Gateway decision (`isPickPossible`) | **Gateway C** → vanilla path / out-of-scope end |
| `picklist_processing_vanilla_pps_node` | Pass-through | — |
| `filter_picklist_node` | Pass-through | — |
| `entity_transport_node` | Pass-through | — |
| `source_operations_node` | Gateway decision (simplified — see below) | **Gateway D** → early-display / slot-pick / item-scan |
| `early_display_item_scan_node` | Placeholder | — |
| `slot_picking_node` | Placeholder | — |
| `wait_for_entity_scan_node` | Message wait (real Kafka) | — |
| `post_scan_node` | Pass-through | — |
| `wait_for_entity_operations_node` | Pass-through | — |
| `wait_for_pick_confirm_node` | Pass-through | — |
| `wait_for_pick_bin_confirm_node` | Message wait (real Kafka, PPTL press) | — |
| `commit_pptl_press` (function) | Write — **STUBBED, see below** | — |
| `wait_for_print_node` | Gateway decision (`printAwaited`) | **Gateway E** → `wait_for_printout_confirm_node` / skip |
| `wait_for_printout_confirm_node` | Placeholder | — |
| `validate_and_process_next_node` (function) | Gateway decision (simplified — see below) | **Gateway F** → `auto_dest_clear_node` / out-of-scope end |
| `auto_dest_clear_node` + `clear_bin_node` | Write (real, combined into one class) | — |
| — (terminal) | Write (real, `releaseRack`) | — |

The pure pass-through nodes are confirmed genuine no-ops for the vanilla
`bin` agent in the real HSM (this session's research) — kept as their own
classes/elements purely for structural parity with butler_server's
one-module-per-node shape.

## Known simplifications (honestly flagged, not silently glossed over)

- **`commit_pptl_press` is stubbed.** The real write
  (`pps_manager:handle_item_picked/23`) needs the full accumulated HSM
  pick-cycle state (checklist data, order/pick details, carrier info), not
  just a PPS/bin id — a genuinely thin wrapper isn't possible without this
  app independently reconstructing that state. The endpoint logs the call
  and returns a canned success response.
- **Gateway D's decision** (`source_operations_node`'s pre-picklist-present
  / slot-pick-possible routing) is hardcoded to always route to
  `wait_for_entity_scan_node` — the real gate reads accumulated HSM state
  flags with no clean butler_server API answer yet.
- **Gateway F's decision** (`validate_and_process_next_node`'s
  single-vs-multi-PPTL-bin routing) is hardcoded to always take the
  single-bin path — same category of problem, plus the multi-bin path is a
  retry loop that's out of scope per an earlier decision anyway.
- **Four placeholder nodes** (`wait_for_dest_clear_node`,
  `wait_for_tote_attachment_node`, `slot_picking_node`,
  `wait_for_printout_confirm_node`) represent real physical-operator
  signals butler_server doesn't yet redirect to Kafka — only rack-arrival,
  PPTL-press, and scan/multiscan are redirected today. They advance
  immediately; a 4th/5th/6th real hand-off redirect would replace them with
  a real message-wait, same pattern as the three already built.
- `picklist_processing_node`'s `false` branch (Gateway C) and
  `validate_and_process_next_node`'s multi-bin branch (Gateway F) both end
  at an explicit "out of scope" end event rather than modeling the real
  HSM's retry loops, per the earlier decision to exclude retry/loop
  constructs from this vanilla-happy-path migration.

## Run it

1. Start your local Zeebe broker / Camunda 8 Run / Operate, and your local Kafka.
2. Deploy `src/main/resources/bpmn/pick_vanilla_migration.bpmn` in Camunda
   Desktop Modeler. Modeler auto-discovers the sibling `element-templates/`
   folder. The auto-generated layout is a plain left-to-right layered graph
   (not hand-tuned) — feel free to drag nodes around, it won't affect execution.
3. Merge the butler_server-side changes to your VM, and turn the flag on
   for a test PPS:
   ```erlang
   pick_camunda_orchestration_config:set_and_persist_enabled_for_pps(PpsId, true).
   ```
4. Build and run:
   ```bash
   mvn -pl camunda-pick-vanilla-migration -am clean install -DskipTests
   java -jar camunda-pick-vanilla-migration/target/camunda-pick-vanilla-migration.jar
   ```
   Override `KAFKA_BOOTSTRAP_SERVERS`, `ZEEBE_GATEWAY_ADDRESS`, and
   `BUTLER_SERVER_BASE_URL` (the VM) as needed.
5. Trigger a real rack arrival / PPTL press / scan for the cut-over PPS and
   watch the instance advance through Operate — the diagram itself should
   now show which gateway branch was actually taken, not just a straight line.

## Known limitations

- The exact `pps_id` field name in the real rack-arrival payload, and the
  precise `is_pick_possible` PickList JSON round-trip, haven't been
  exercised against a live butler_server yet.
- Element templates are hand-authored against the documented Camunda 8
  schema, not yet opened in a live Modeler to confirm validity.
- The BPMN's auto-generated layout is functional but not visually polished.
