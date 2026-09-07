package com.greyorange.camunda.l1;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.Instant;

/**
 * L1 Data — base JPA entity for all Camunda-backed workflow entities.
 *
 * Every workflow entity extends this class. It provides the mandatory base fields
 * that every flow needs for correlation, auditing, and status tracking.
 *
 * MANDATORY vs OPTIONAL FIELD CONVENTION:
 *   Fields in this base class are all mandatory — they are non-nullable and
 *   always populated before any entity reaches L2.
 *   Fields in subclasses should be annotated accordingly:
 *     - Mandatory → @Column(nullable = false)   and  @WorkflowDto.Mandatory on the DTO
 *     - Optional  → @Column(nullable = true)    and  @WorkflowDto.Optional  on the DTO
 *
 * DAL PIPELINE:
 *   Raw data from any source (DB, HTTP, Kafka, InfluxDB, File) is first mapped to a
 *   WorkflowDto by a WorkflowDal implementation. The DAL validates mandatory DTO fields,
 *   then calls toEntity() to produce a WorkflowEntityBase subclass. No L2 or L3 code
 *   ever constructs entity objects directly from raw source data.
 *
 * MDC / CONTEXT:
 *   WorkflowContext is populated by the Zeebe worker before any entity is touched
 *   and cleared in the finally block. tenantId and workflowId here mirror the
 *   WorkflowContext fields for SQL-level filtering.
 *
 * EXAMPLE:
 *
 *   @Entity
 *   @Table(name = "pick_front_entity")
 *   public class PickFrontEntity extends WorkflowEntityBase {
 *
 *       @Column(nullable = false)           // mandatory
 *       private String orderId;
 *
 *       @Column(nullable = false)           // mandatory
 *       private String ppsId;
 *
 *       @Column(nullable = true)            // optional
 *       private String containerId;
 *   }
 *
 * Column names mirror Zeebe ioMapping variable names for cross-stack consistency.
 */
@MappedSuperclass
@Getter
@Setter
public abstract class WorkflowEntityBase {

    /** Primary key — business key or UUID; set by the DAL before persist. */
    @Id
    @Column(name = "id", nullable = false)
    private String id;

    /** Zeebe process instance key — correlates this DB row to a live BPMN instance. */
    @Column(name = "process_id")
    private String processId;

    /** Tenant / warehouse identifier — matches WorkflowContext.tenantId. */
    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    /** Source from which this entity was originally populated. Informational. */
    @Enumerated(EnumType.STRING)
    @Column(name = "dal_source")
    private DalSource dalSource;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private WorkflowStatus status = WorkflowStatus.PENDING;

    /** Optimistic-lock version — prevents lost-update on concurrent saga commits. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
