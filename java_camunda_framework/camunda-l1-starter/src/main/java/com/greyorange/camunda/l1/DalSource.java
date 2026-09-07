package com.greyorange.camunda.l1;

/**
 * L1 · DAL Source
 *
 * Identifies the physical source from which a DTO was fetched.
 * Passed into WorkflowDal implementations and recorded on WorkflowContext.
 *
 * Used for routing, observability, and conditional transformation logic
 * (e.g. a Kafka-sourced DTO may skip DB validation steps).
 */
public enum DalSource {

    /** Relational DB or JPA repository. */
    DB,

    /** HTTP / REST endpoint or HTML form payload. */
    HTTP,

    /** Kafka consumer record. */
    KAFKA,

    /** InfluxDB time-series measurement. */
    INFLUX,

    /** File system — CSV, JSON, XML, etc. */
    FILE
}
