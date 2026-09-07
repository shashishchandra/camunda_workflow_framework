package com.greyorange.demo;

import com.greyorange.camunda.l1.WorkflowStatus;
import com.greyorange.camunda.l2.WorkflowMq;
import com.greyorange.camunda.l2.WorkflowRule;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import com.greyorange.camunda.l2.WorkflowSagaBase;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * L2 — the master_pptl_press saga. Rule enforces that a rack must already be
 * DOCKED (written by RackArrivedSaga) before the PPTL button press is accepted —
 * the same "validation before orchestration" invariant as the Erlang L2 layer.
 */
@Service
public class MasterPptlPressSaga extends WorkflowSagaBase {

    private static final WorkflowRule<RackEntity> RACK_MUST_BE_DOCKED = WorkflowRule.of(
        "rack_must_be_docked",
        "Master PPTL cannot be pressed before the rack has been marked arrived",
        rack -> rack != null && "DOCKED".equals(rack.getRackStatus())
    );

    private final RackRepository racks;

    public MasterPptlPressSaga(WorkflowMq mq, WorkflowRuleEngine ruleEngine, EntityManager em, RackRepository racks) {
        super(mq, ruleEngine, em);
        this.racks = racks;
    }

    @Transactional
    public Map<String, Object> pressMasterPptl(String workOrderId) {
        RackEntity rack = racks.findById(workOrderId).orElse(null);
        return runSaga(
            rack,
            List.of(RACK_MUST_BE_DOCKED),
            () -> {
                rack.setRackStatus("BIN_LIT");
                rack.setStatus(WorkflowStatus.COMPLETED);
                racks.save(rack);
                return Map.of(
                    "bin_light", "ON",
                    "pptl_pressed_at", Instant.now().toString()
                );
            },
            "demo.events",
            "Task_MasterPptlPress"
        );
    }
}
