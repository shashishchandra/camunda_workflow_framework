package com.greyorange.demo;

import com.greyorange.camunda.l1.DalSource;
import com.greyorange.camunda.l1.WorkflowStatus;
import com.greyorange.camunda.l2.WorkflowMq;
import com.greyorange.camunda.l2.WorkflowRule;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import com.greyorange.camunda.l2.WorkflowSagaBase;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * L2 — the rack_arrived saga. Same runSaga() contract as every other
 * WorkflowSagaBase subclass: rule check (before any write) -> commit (inside
 * @Transactional) -> checkpoint (same tx) -> publish (only after commit).
 */
@Service
public class RackArrivedSaga extends WorkflowSagaBase {

    private static final Logger log = LoggerFactory.getLogger(RackArrivedSaga.class);

    private static final WorkflowRule<String> WORK_ORDER_PRESENT = WorkflowRule.of(
        "work_order_present",
        "workOrderId must be provided before a rack can be marked arrived",
        workOrderId -> workOrderId != null && !workOrderId.isBlank()
    );

    private final RackRepository racks;

    public RackArrivedSaga(WorkflowMq mq, WorkflowRuleEngine ruleEngine, EntityManager em, RackRepository racks) {
        super(mq, ruleEngine, em);
        this.racks = racks;
    }

    @Transactional
    public Map<String, Object> markArrived(String workOrderId) {
        return runSaga(
            workOrderId,
            List.of(WORK_ORDER_PRESENT),
            () -> {
                RackEntity rack = racks.findById(workOrderId).orElseGet(RackEntity::new);
                rack.setId(workOrderId);
                rack.setTenantId("demo");
                rack.setDalSource(DalSource.KAFKA); // in prod this arrives via Kafka, like rack_arrived today
                rack.setStatus(WorkflowStatus.IN_PROGRESS);
                rack.setRackStatus("DOCKED");
                racks.save(rack);
                return Map.of(
                    "rack_status", "DOCKED",
                    "rack_arrived_at", Instant.now().toString()
                );
            },
            "demo.events",
            "Task_RackArrived"
        );
    }

    @Override
    protected void onRollback(Throwable cause) {
        log.error("rack_arrived saga rolled back: {}", cause.getMessage());
    }
}
