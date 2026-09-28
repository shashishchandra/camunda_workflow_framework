package com.greyorange.pickvanillademo;

import com.greyorange.camunda.l1.WorkflowEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * L1 -- one row per pick instruction, per outer-loop iteration. A fresh row is created
 * each time the loop starts a new instruction for a rack (see PickCycleSaga /
 * PickInstructionSaga), rather than one entity trying to advance through every
 * instruction on the same row -- mirroring the real HSM resetting its local state via
 * get_resetted_hsm_state_v2/1 and re-entering waiting_for_pick_start_node fresh for
 * each instruction. id is a generated value (NOT ppsId), since many rows can exist
 * for the same rack over its lifetime; ppsId links each row back to its PickCycleEntity.
 */
@Entity
@Table(name = "pick_instruction")
@Getter
@Setter
public class PickInstructionEntity extends WorkflowEntityBase {

    @Column(name = "pps_id", nullable = false)
    private String ppsId;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_phase", nullable = false)
    private PickInstructionPhase currentPhase;
}
