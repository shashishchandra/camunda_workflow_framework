package com.greyorange.camunda.l1;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.Instant;

/**
 * L1 Data — base JPA entity for all Camunda-backed workflow entities.
 *
 * Extend this in your service-specific entity:
 *
 *   @Entity
 *   @Table(name = "pick_front_entity")
 *   public class PickFrontEntity extends WorkflowEntityBase {
 *       @Column private String orderId;
 *       @Column private String ppsId;
 *       @Column private String scanResult;
 *   }
 *
 * The base class provides: id, processId, tenantId, status, created/updated timestamps.
 * Column names mirror the Zeebe ioMapping variable names for cross-stack consistency.
 */
@MappedSuperclass
@Getter
@Setter
public abstract class WorkflowEntityBase {

    @Id
    @Column(name = "id", nullable = false)
    private String id;

    /** Zeebe process instance key (as string) — correlates DB row to BPMN instance. */
    @Column(name = "process_id")
    private String processId;

    @Column(name = "tenant_id")
    private String tenantId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private WorkflowStatus status = WorkflowStatus.PENDING;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
