package com.greyorange.pickvanillademo.connector;

import com.greyorange.camunda.l2.WorkflowRule;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import com.greyorange.camunda.l3.AbstractOperationConnector;
import com.greyorange.pickvanillademo.ButlerServerApiClient;
import com.greyorange.pickvanillademo.PickCycleSaga;
import io.camunda.connector.api.annotation.Operation;
import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.annotation.Variable;
import io.camunda.connector.generator.java.annotation.ElementTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static com.greyorange.pickvanillademo.PickCyclePhase.AUTO_DEST_CLEAR;
import static com.greyorange.pickvanillademo.PickCyclePhase.COMMIT_PPTL_PRESS;
import static com.greyorange.pickvanillademo.PickCyclePhase.PICKLIST_PROCESSING;
import static com.greyorange.pickvanillademo.PickCyclePhase.PICK_PRE_REQUISITES;
import static com.greyorange.pickvanillademo.PickCyclePhase.WAIT_FOR_PICK_BIN_CONFIRM;
import static com.greyorange.pickvanillademo.PickCyclePhase.WAIT_FOR_PRINT;
import static com.greyorange.pickvanillademo.PickCyclePhase.WAIT_FOR_SOURCE_ARRIVAL;
import static com.greyorange.pickvanillademo.PickCyclePhase.WAIT_FOR_WORK_ALLOCATION;
import static com.greyorange.pickvanillademo.PickCyclePhase.WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS;

/**
 * The entire L3-equivalent for this demo's 8 butler_server-backed HSM nodes: one
 * {@code @OutboundConnector}, one {@code @Operation} per node. There is no
 * hand-rolled job-worker anywhere -- the Connector SDK's own runtime handles job
 * polling, completion, failure and retries the moment it sees {@code @Operation}.
 * Each method's body is exactly the shape {@link AbstractOperationConnector}
 * captures: guardrail (L2 WorkflowRuleEngine) -> real butler_server read
 * (ButlerServerApiClient) -> persisted phase advance (L2 PickCycleSaga -> L1
 * PickCycleEntity) -> merged result map. The base class runs that shape; each
 * method below only supplies what's actually different -- which guardrails,
 * which call, which phase.
 *
 * WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS is one real HSM node that makes two
 * butler_server calls (hasClearableFrontBin, isFrontToteFlow) -- split here into
 * two operations/BPMN tasks so each gets its own connector job, but both target
 * the same phase; PickCycleSaga's monotonic advance makes the second call a safe
 * no-op re-affirmation rather than a duplicate transition.
 *
 * Extends {@link AbstractOperationConnector}, which itself implements only
 * {@code OutboundConnectorProvider} (an empty marker) -- that's what tells both
 * the runtime and the element-template generator "this is an Operations API
 * class, discover its behavior via {@code @Operation} reflection".
 *
 * Spring auto-wires this bean directly into DefaultOutboundConnectorFactory's
 * {@code List<OutboundConnectorProvider>} constructor parameter -- no
 * ServiceLoader/SPI involved for that path. The 8.9.12 annotation processor
 * ALSO unconditionally emits a
 * {@code META-INF/services/io.camunda.connector.api.outbound.OutboundConnectorFunction}
 * entry for every {@code @OutboundConnector} class regardless of which
 * interface it actually implements; DefaultOutboundConnectorFactory separately
 * runs a raw {@code ServiceLoader.load(OutboundConnectorFunction.class)} scan
 * that picks that stray entry up and tries to instantiate it via a no-arg
 * constructor, which this class doesn't have (it's constructor-injected). The
 * pom's antrun step deletes that generated file before packaging so only the
 * correct, Spring-injected discovery path is ever used -- confirmed by
 * actually running the app and reading both the crash and the generated
 * META-INF/services files inside the jar, not assumed.
 */
@Component
@OutboundConnector(
    name = "Pick Vanilla Demo -- butler_server",
    type = "io.greyorange.pickvanillademo:butler-server:1",
    inputVariables = {"pps_id", "bin_id", "pick_list"})
@ElementTemplate(
    id = "io.greyorange.pickvanillademo.ButlerServer.v1",
    name = "Pick Vanilla Demo -- butler_server",
    version = 1,
    description = "Read-only calls into butler_server's pick_vanilla_demo_http_handler, "
        + "one operation per pick-front vanilla HSM node. Never writes.")
public class PickVanillaDemoOutboundConnector extends AbstractOperationConnector {

    private static final WorkflowRule<String> PPS_ID_PRESENT = WorkflowRule.of(
        "pps_id_present",
        "pps_id must be provided before calling butler_server",
        ppsId -> ppsId != null && !ppsId.isBlank());

    private final ButlerServerApiClient api;
    private final PickCycleSaga saga;

    public PickVanillaDemoOutboundConnector(ButlerServerApiClient api, PickCycleSaga saga, WorkflowRuleEngine ruleEngine) {
        super(ruleEngine);
        this.api = api;
        this.saga = saga;
    }

    @Operation(id = "has_clearable_front_bin", name = "Has Clearable Front Bin",
        description = "wait_for_work_allocation_vanilla_pps_node: is there a full bin to clear first?")
    public Map<String, Object> hasClearableFrontBin(@Variable("pps_id") String ppsId) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("has_clearable_bin", api.hasClearableFrontBin(ppsId)),
            () -> saga.advancePhase(ppsId, WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS, WAIT_FOR_SOURCE_ARRIVAL, "Op_HasClearableFrontBin"));
    }

    @Operation(id = "is_front_tote_flow", name = "Is Front Tote Flow",
        description = "wait_for_work_allocation_vanilla_pps_node: does this PPS need a tote attached?")
    public Map<String, Object> isFrontToteFlow(@Variable("pps_id") String ppsId) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("is_front_tote_flow", api.isFrontToteFlow(ppsId)),
            () -> saga.advancePhase(ppsId, WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS, WAIT_FOR_WORK_ALLOCATION, "Op_IsFrontToteFlow"));
    }

    @Operation(id = "is_destination_orchestrated_by_htm", name = "Pick Prerequisites",
        description = "pick_pre_requisites_node: is the destination orchestrated by HTM?")
    public Map<String, Object> isDestinationOrchestratedByHtm(@Variable("pps_id") String ppsId) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("is_destination_orchestrated_by_htm", api.isDestinationOrchestratedByHtm(ppsId)),
            () -> saga.advancePhase(ppsId, PICK_PRE_REQUISITES, WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS, "Op_PickPreRequisites"));
    }

    @Operation(id = "is_pick_possible", name = "Picklist Processing",
        description = "picklist_processing_node: is the picklist actually workable right now?")
    public Map<String, Object> isPickPossible(
        @Variable("pps_id") String ppsId,
        @Variable(value = "pick_list", required = false) List<Map<String, Object>> pickList
    ) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("is_pick_possible", api.isPickPossible(ppsId, pickList == null ? List.of() : pickList)),
            () -> saga.advancePhase(ppsId, PICKLIST_PROCESSING, PICK_PRE_REQUISITES, "Op_PicklistProcessing"));
    }

    @Operation(id = "print_awaited", name = "Wait For Print",
        description = "wait_for_print_node: is a dock station awaiting print for this PPS?")
    public Map<String, Object> printAwaited(@Variable("pps_id") String ppsId) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("print_awaited", !api.printAwaitedDockStationIds(ppsId).isEmpty()),
            () -> saga.advancePhase(ppsId, WAIT_FOR_PRINT, COMMIT_PPTL_PRESS, "Op_WaitForPrint"));
    }

    @Operation(id = "commit_pptl_press", name = "Commit PPTL Press (log only)",
        description = "wait_for_pick_bin_confirm_node's commit_pptl_press/2. No real write -- "
            + "reads the bin's real current state and logs what would commit.")
    public Map<String, Object> commitPptlPress(
        @Variable("pps_id") String ppsId,
        @Variable("bin_id") String binId
    ) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("bin_state_at_commit", api.ppsBinDetails(ppsId, binId)),
            () -> saga.advancePhase(ppsId, COMMIT_PPTL_PRESS, WAIT_FOR_PICK_BIN_CONFIRM, "Op_CommitPptlPress"));
    }

    @Operation(id = "auto_dest_clear", name = "Auto Dest Clear (log only)",
        description = "auto_dest_clear_node / clear_bin_node. No real write -- reads the bin's "
            + "real current state and logs what would clear.")
    public Map<String, Object> autoDestClear(
        @Variable("pps_id") String ppsId,
        @Variable("bin_id") String binId
    ) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("bin_state_at_clear", api.ppsBinDetails(ppsId, binId)),
            () -> saga.advancePhase(ppsId, AUTO_DEST_CLEAR, WAIT_FOR_PRINT, "Op_AutoDestClear"));
    }

    @Operation(id = "rack_released", name = "Rack Released (log only)",
        description = "pick_front_hsm.erl's terminal entity_transport_node(endproc,...). No "
            + "real write -- reads the PPS's real current occupancy, completes the cycle.")
    public Map<String, Object> rackReleased(@Variable("pps_id") String ppsId) {
        return runOperation(
            ppsId, List.of(PPS_ID_PRESENT),
            () -> Map.of("pps_state_at_release", api.ppsDetails(ppsId)),
            () -> saga.completeCycle(ppsId, AUTO_DEST_CLEAR, "Op_RackReleased"));
    }
}
