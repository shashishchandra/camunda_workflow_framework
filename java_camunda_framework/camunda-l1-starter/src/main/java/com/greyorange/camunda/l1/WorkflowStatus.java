package com.greyorange.camunda.l1;

/** Shared status enum for all workflow entities. Extend or override in your service. */
public enum WorkflowStatus {
    PENDING, IN_PROGRESS, COMPLETED, FAILED, SIDELINED
}
