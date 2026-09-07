package com.greyorange.demo;

import com.greyorange.camunda.l1.WorkflowEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * L1 — the one domain entity this demo owns. Extends WorkflowEntityBase so it
 * inherits tenant scoping, optimistic-lock versioning, and audit timestamps for free.
 * id = the work order id; rackStatus tracks DOCKED -> BIN_LIT across the two demo tasks.
 */
@Entity
@Table(name = "demo_rack")
@Getter
@Setter
public class RackEntity extends WorkflowEntityBase {

    @Column(name = "rack_status", nullable = false)
    private String rackStatus;
}
