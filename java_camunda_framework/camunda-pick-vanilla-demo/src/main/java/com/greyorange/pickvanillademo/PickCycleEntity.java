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
 * L1 -- one row per PPS's current pick cycle. id = ppsId (one rack fully
 * occupies a PPS end to end in the vanilla path, so ppsId alone is a safe
 * correlation key here).
 */
@Entity
@Table(name = "pick_cycle")
@Getter
@Setter
public class PickCycleEntity extends WorkflowEntityBase {

    @Enumerated(EnumType.STRING)
    @Column(name = "current_phase", nullable = false)
    private PickCyclePhase currentPhase;
}
