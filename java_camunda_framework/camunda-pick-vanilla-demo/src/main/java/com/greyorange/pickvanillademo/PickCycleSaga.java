package com.greyorange.pickvanillademo;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.greyorange.camunda.l1.DalSource;
import com.greyorange.camunda.l1.WorkflowStatus;
import com.greyorange.camunda.l2.WorkflowMq;
import com.greyorange.camunda.l2.WorkflowRule;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import com.greyorange.camunda.l2.WorkflowSagaBase;

import jakarta.persistence.EntityManager;

/**
 * L2 - one saga per PPS pick cycle, one method per node boundary
 * (PickCyclePhase). Same runSaga() contract as every WorkflowSagaBase
 * subclass: rule check -> commit (inside @Transactional) -> checkpoint
 * (same tx) -> publish (only after commit, here just PickVanillaLoggingMq).
 *
 * advancePhase takes an explicit `minimumPriorPhase` rather than assuming
 * `phase.ordinal() - 1`, because several phases are branch ALTERNATIVES
 * (only one of WAIT_FOR_DEST_CLEAR/WAIT_FOR_TOTE_ATTACHMENT fires per
 * instance, for example) -> the node immediately after a branch group must
 * accept whichever alternative actually ran, not assume a fixed strict
 * predecessor.
 *
 * Today every branch alternative in this flow is a cosmetic script task that
 * never persists its own phase, so a single minimumPriorPhase (the phase
 * shared by both alternatives, before the branch) is always enough. The
 * Set<PickCyclePhase> overloads below exist for the case a future workflow
 * needs: two or more alternatives that each persist their OWN distinct
 * phase, merging into a shared downstream step. The guard accepts the
 * downstream step once the cycle's phase is at or past ANY one of the given
 * phases -- not exact equality, so redelivery/monotonic-advance safety is
 * preserved independently within whichever branch actually ran.
 */
@Service
public class PickCycleSaga extends WorkflowSagaBase {

    private static final Logger log = LoggerFactory.getLogger(PickCycleSaga.class);
    private static final String DESTINATION = "pick.vanilla_migration.observed_events";

    private final PickCycleRepository cycles;

    public PickCycleSaga(WorkflowMq mq, WorkflowRuleEngine ruleEngine, EntityManager em, PickCycleRepository cycles) {
        super(mq, ruleEngine, em);
        this.cycles = cycles;
    }

    private static final WorkflowRule<String> PPS_ID_PRESENT = WorkflowRule.of(
        "pps_id_present",
        "ppsId must be provided before a pick cycle can start",
        ppsId -> ppsId != null && !ppsId.isBlank()
    );

    /** Creates (or resets, on redelivery) the pick cycle row for ppsId at WAIT_FOR_SOURCE_ARRIVAL. */
    @Transactional
    public Map<String, Object> waitForSourceArrival(String ppsId) {
        return runSaga(
            ppsId,
            List.of(PPS_ID_PRESENT),
            () -> {
                PickCycleEntity cycle = cycles.findById(ppsId).orElseGet(PickCycleEntity::new);
                cycle.setId(ppsId);
                cycle.setTenantId("migration");
                cycle.setDalSource(DalSource.KAFKA);
                cycle.setStatus(WorkflowStatus.IN_PROGRESS);
                advancePhase(cycle, PickCyclePhase.WAIT_FOR_SOURCE_ARRIVAL);
                cycles.save(cycle);
                return Map.of("pps_id", ppsId, "phase", cycle.getCurrentPhase().name());
            },
            DESTINATION,
            "Task_WaitForSourceArrival"
        );
    }

    /**
     * Generic advance for any intermediate node, accepting a SET of phases any one of which
     * satisfies the predecessor guard -- for a merge point downstream of several branches that
     * each persist their own distinct phase, pass all of them here. For the common single-branch
     * case (a node right after a branch group whose alternatives don't persist a distinct phase
     * of their own), use the {@code PickCyclePhase} overload below instead.
     */
    @Transactional
    public Map<String, Object> advancePhase(
        String ppsId, PickCyclePhase phase, Set<PickCyclePhase> acceptableMinimumPriorPhases, String checkpointStep
    ) {
        PickCycleEntity cycle = requireCycle(ppsId);
        WorkflowRule<PickCycleEntity> reachedPriorPhase = priorPhaseReached(phase, acceptableMinimumPriorPhases);
        return runSaga(
            cycle,
            List.of(reachedPriorPhase),
            () -> {
                advancePhase(cycle, phase);
                cycles.save(cycle);
                return Map.of("pps_id", ppsId, "phase", cycle.getCurrentPhase().name());
            },
            DESTINATION,
            checkpointStep
        );
    }

    /**
     * Convenience overload for the common case: exactly one predecessor phase (or one branch
     * group whose alternatives all share the same phase before the branch). See the
     * Set<PickCyclePhase> overload above for a merge point fed by several distinctly-persisted
     * branch phases.
     */
    @Transactional
    public Map<String, Object> advancePhase(
        String ppsId, PickCyclePhase phase, PickCyclePhase minimumPriorPhase, String checkpointStep
    ) {
        return advancePhase(ppsId, phase, Set.of(minimumPriorPhase), checkpointStep);
    }

    /** Advances to the terminal RACK_RELEASED phase and marks the cycle COMPLETED. */
    @Transactional
    public Map<String, Object> completeCycle(String ppsId, Set<PickCyclePhase> acceptableMinimumPriorPhases, String checkpointStep) {
        PickCycleEntity cycle = requireCycle(ppsId);
        WorkflowRule<PickCycleEntity> reachedPriorPhase = priorPhaseReached(PickCyclePhase.RACK_RELEASED, acceptableMinimumPriorPhases);
        return runSaga(
            cycle,
            List.of(reachedPriorPhase),
            () -> {
                advancePhase(cycle, PickCyclePhase.RACK_RELEASED);
                cycle.setStatus(WorkflowStatus.COMPLETED);
                cycles.save(cycle);
                return Map.of("pps_id", ppsId, "phase", cycle.getCurrentPhase().name());
            },
            DESTINATION,
            checkpointStep
        );
    }

    /** Convenience overload for the common single-predecessor case. */
    @Transactional
    public Map<String, Object> completeCycle(String ppsId, PickCyclePhase minimumPriorPhase, String checkpointStep) {
        return completeCycle(ppsId, Set.of(minimumPriorPhase), checkpointStep);
    }

    @Override
    protected void onRollback(Throwable cause) {
        log.error("pick cycle saga rolled back: {}", cause.getMessage());
    }

    // ─── shared phase-advance plumbing ──────────────────────────────────────

    private PickCycleEntity requireCycle(String ppsId) {
        return cycles.findById(ppsId).orElseThrow(
            () -> new IllegalStateException("No pick cycle found for ppsId " + ppsId + " -- wait_for_source_arrival must be observed first")
        );
    }

    /** Only moves currentPhase forward -- redelivery of an already-applied phase is a safe no-op. */
    private void advancePhase(PickCycleEntity cycle, PickCyclePhase phase) {
        if (cycle.getCurrentPhase() == null || phase.ordinal() > cycle.getCurrentPhase().ordinal()) {
            cycle.setCurrentPhase(phase);
        }
    }

    /**
     * Satisfied once the cycle's current phase is at or past ANY one of the given phases --
     * never exact equality, so redelivery/monotonic-advance safety holds independently within
     * whichever branch actually ran, even when several distinctly-persisted branches merge here.
     */
    private WorkflowRule<PickCycleEntity> priorPhaseReached(PickCyclePhase phase, Set<PickCyclePhase> acceptableMinimumPriorPhases) {
        return WorkflowRule.of(
            phase.name().toLowerCase() + "_prior_phase_reached",
            "Cycle must have reached at least one of " + acceptableMinimumPriorPhases + " before " + phase,
            cycle -> cycle.getCurrentPhase() != null
                && acceptableMinimumPriorPhases.stream().anyMatch(p -> cycle.getCurrentPhase().ordinal() >= p.ordinal())
        );
    }
}
