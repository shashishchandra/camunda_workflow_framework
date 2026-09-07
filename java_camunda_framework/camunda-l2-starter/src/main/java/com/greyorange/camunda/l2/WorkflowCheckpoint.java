package com.greyorange.camunda.l2;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * L2 · Checkpoint Entity
 *
 * Records a named resume point for a workflow instance. Checkpoints capture the
 * current variable state at a named step so that a crashed or failed worker can
 * restart from that point rather than replaying the entire flow from scratch.
 *
 * WRITE PATH:
 *   WorkflowSagaBase.checkpoint(step, variables) persists one row inside the
 *   same DB transaction as the business commit. If the transaction rolls back,
 *   the checkpoint row is also rolled back — there are never orphaned checkpoints.
 *
 * READ PATH:
 *   On re-entry a worker can query: SELECT * FROM workflow_checkpoints
 *   WHERE workflow_id = :id AND tenant_id = :tenant ORDER BY created_at DESC LIMIT 1
 *   to resume from the last known-good step.
 *
 * TABLE: workflow_checkpoints
 */
@Entity
@Table(
    name = "workflow_checkpoints",
    indexes = {
        @Index(name = "idx_wcp_workflow", columnList = "workflow_id, tenant_id"),
        @Index(name = "idx_wcp_step",     columnList = "workflow_id, step")
    }
)
@Getter
@Setter
@NoArgsConstructor
public class WorkflowCheckpoint {

    /** Surrogate primary key — auto-generated. */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", nullable = false, updatable = false)
    private String id;

    /**
     * Business / correlation key of the workflow this checkpoint belongs to.
     * Matches WorkflowEntityBase.id and WorkflowContext.workflowId.
     */
    @Column(name = "workflow_id", nullable = false)
    private String workflowId;

    /** Tenant / warehouse identifier. Matches WorkflowContext.tenantId. */
    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    /**
     * Named step at which this checkpoint was saved.
     * Recommended convention: use the BPMN task ID (e.g. "Task_ScanBarcode")
     * so checkpoints are directly traceable to diagram elements.
     */
    @Column(name = "step", nullable = false)
    private String step;

    /**
     * JSON-serialised snapshot of workflow variables at this step.
     * Use ObjectMapper to serialize the variables map before storing.
     * Max 64 KB (TEXT column); for larger payloads, store a reference (S3 key, etc.).
     */
    @Column(name = "variables_json", columnDefinition = "TEXT")
    private String variablesJson;

    /** Wall-clock timestamp when this checkpoint was persisted. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    public WorkflowCheckpoint(String workflowId, String tenantId, String step, String variablesJson) {
        this.workflowId    = workflowId;
        this.tenantId      = tenantId;
        this.step          = step;
        this.variablesJson = variablesJson;
    }
}
