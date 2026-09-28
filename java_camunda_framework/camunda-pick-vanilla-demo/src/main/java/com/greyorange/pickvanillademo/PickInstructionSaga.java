package com.greyorange.pickvanillademo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
 * L2 - the per-pick-instruction counterpart to PickCycleSaga. Same runSaga()
 * contract, same monotonic-advance/prior-phase-reached guard shape -- only scoped to
 * PickInstructionPhase instead of PickCyclePhase, and resolving the CURRENT
 * in-progress instruction row for a given ppsId rather than taking an explicit
 * instruction id, since only one instruction is ever active per rack at a time (the
 * outer loop processes them sequentially, not in parallel). Callers keep passing
 * plain ppsId, exactly as before this split -- what changed is which saga/entity
 * they land on underneath, not their own method signatures.
 */
@Service
public class PickInstructionSaga extends WorkflowSagaBase {

    private static final Logger log = LoggerFactory.getLogger(PickInstructionSaga.class);
    private static final String DESTINATION = "pick.vanilla_migration.observed_events";

    private final PickInstructionRepository instructions;

    public PickInstructionSaga(WorkflowMq mq, WorkflowRuleEngine ruleEngine, EntityManager em, PickInstructionRepository instructions) {
        super(mq, ruleEngine, em);
        this.instructions = instructions;
    }

    private static final WorkflowRule<String> PPS_ID_PRESENT = WorkflowRule.of(
        "pps_id_present",
        "ppsId must be provided before a pick instruction can start",
        ppsId -> ppsId != null && !ppsId.isBlank()
    );

    /** Starts a fresh pick instruction for ppsId -- one new row per outer-loop iteration. */
    @Transactional
    public Map<String, Object> startInstruction(String ppsId) {
        return runSaga(
            ppsId,
            List.of(PPS_ID_PRESENT),
            () -> {
                PickInstructionEntity instruction = new PickInstructionEntity();
                instruction.setId(UUID.randomUUID().toString());
                instruction.setPpsId(ppsId);
                instruction.setTenantId("migration");
                instruction.setDalSource(DalSource.KAFKA);
                instruction.setStatus(WorkflowStatus.IN_PROGRESS);
                advancePhase(instruction, PickInstructionPhase.WAITING_FOR_CARRIER_VALIDATION);
                instructions.save(instruction);
                return Map.of("pps_id", ppsId, "phase", instruction.getCurrentPhase().name());
            },
            DESTINATION,
            "Task_StartPickInstruction"
        );
    }

    /** See PickCycleSaga's Set<PickCyclePhase> overload -- identical reasoning, scoped to instructions. */
    @Transactional
    public Map<String, Object> advancePhase(
        String ppsId, PickInstructionPhase phase, Set<PickInstructionPhase> acceptableMinimumPriorPhases, String checkpointStep
    ) {
        PickInstructionEntity instruction = requireCurrentInstruction(ppsId);
        WorkflowRule<PickInstructionEntity> reachedPriorPhase = priorPhaseReached(phase, acceptableMinimumPriorPhases);
        return runSaga(
            instruction,
            List.of(reachedPriorPhase),
            () -> {
                advancePhase(instruction, phase);
                instructions.save(instruction);
                return Map.of("pps_id", ppsId, "phase", instruction.getCurrentPhase().name());
            },
            DESTINATION,
            checkpointStep
        );
    }

    /** Convenience overload for the common single-predecessor case. */
    @Transactional
    public Map<String, Object> advancePhase(
        String ppsId, PickInstructionPhase phase, PickInstructionPhase minimumPriorPhase, String checkpointStep
    ) {
        return advancePhase(ppsId, phase, Set.of(minimumPriorPhase), checkpointStep);
    }

    /** Advances to the terminal VALIDATE_AND_PROCESS_NEXT phase and marks the instruction COMPLETED. */
    @Transactional
    public Map<String, Object> completeInstruction(String ppsId, Set<PickInstructionPhase> acceptableMinimumPriorPhases, String checkpointStep) {
        PickInstructionEntity instruction = requireCurrentInstruction(ppsId);
        WorkflowRule<PickInstructionEntity> reachedPriorPhase =
            priorPhaseReached(PickInstructionPhase.VALIDATE_AND_PROCESS_NEXT, acceptableMinimumPriorPhases);
        return runSaga(
            instruction,
            List.of(reachedPriorPhase),
            () -> {
                advancePhase(instruction, PickInstructionPhase.VALIDATE_AND_PROCESS_NEXT);
                instruction.setStatus(WorkflowStatus.COMPLETED);
                instructions.save(instruction);
                return Map.of("pps_id", ppsId, "phase", instruction.getCurrentPhase().name());
            },
            DESTINATION,
            checkpointStep
        );
    }

    /** Convenience overload for the common single-predecessor case. */
    @Transactional
    public Map<String, Object> completeInstruction(String ppsId, PickInstructionPhase minimumPriorPhase, String checkpointStep) {
        return completeInstruction(ppsId, Set.of(minimumPriorPhase), checkpointStep);
    }

    @Override
    protected void onRollback(Throwable cause) {
        log.error("pick instruction saga rolled back: {}", cause.getMessage());
    }

    // ─── shared phase-advance plumbing ──────────────────────────────────────

    private PickInstructionEntity requireCurrentInstruction(String ppsId) {
        return instructions.findFirstByPpsIdAndStatusOrderByCreatedAtDesc(ppsId, WorkflowStatus.IN_PROGRESS).orElseThrow(
            () -> new IllegalStateException("No in-progress pick instruction found for ppsId " + ppsId + " -- start_instruction must be observed first")
        );
    }

    /** Only moves currentPhase forward -- redelivery of an already-applied phase is a safe no-op. */
    private void advancePhase(PickInstructionEntity instruction, PickInstructionPhase phase) {
        if (instruction.getCurrentPhase() == null || phase.ordinal() > instruction.getCurrentPhase().ordinal()) {
            instruction.setCurrentPhase(phase);
        }
    }

    /** Same semantics as PickCycleSaga's priorPhaseReached -- see there for the full rationale. */
    private WorkflowRule<PickInstructionEntity> priorPhaseReached(PickInstructionPhase phase, Set<PickInstructionPhase> acceptableMinimumPriorPhases) {
        return WorkflowRule.of(
            phase.name().toLowerCase() + "_prior_phase_reached",
            "Instruction must have reached at least one of " + acceptableMinimumPriorPhases + " before " + phase,
            instruction -> instruction.getCurrentPhase() != null
                && acceptableMinimumPriorPhases.stream().anyMatch(p -> instruction.getCurrentPhase().ordinal() >= p.ordinal())
        );
    }
}
