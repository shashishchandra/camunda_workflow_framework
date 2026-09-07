package com.greyorange.camunda.l1;

import org.slf4j.MDC;

import java.util.Map;
import java.util.UUID;

/**
 * L1 · MDC Context Middleware
 *
 * Thread-local correlation context propagated across all layers.
 * Populated at the entry point (Zeebe worker, Kafka consumer, HTTP handler)
 * and cleared on exit. All structured log fields are derived from here.
 *
 * USAGE (in a Zeebe worker or Kafka listener):
 *
 *   WorkflowContext.set(WorkflowContext.of(job.getVariables()));
 *   try {
 *       saga.commitScan(...);
 *   } finally {
 *       WorkflowContext.clear();
 *   }
 *
 * Fields are also written to MDC so every log line carries them automatically
 * without explicit parameterisation.
 */
public final class WorkflowContext {

    // MDC key constants — use these in logback/log4j pattern: %X{workflowId}
    public static final String KEY_WORKFLOW_ID    = "workflowId";
    public static final String KEY_TENANT_ID      = "tenantId";
    public static final String KEY_CORRELATION_ID = "correlationId";
    public static final String KEY_SOURCE         = "source";
    public static final String KEY_STEP           = "step";

    private static final ThreadLocal<WorkflowContext> HOLDER = new ThreadLocal<>();

    private final String workflowId;
    private final String tenantId;
    private final String correlationId;
    private final String source;
    private String       step;

    private WorkflowContext(String workflowId, String tenantId,
                            String correlationId, String source) {
        this.workflowId    = workflowId;
        this.tenantId      = tenantId;
        this.correlationId = correlationId;
        this.source        = source;
    }

    /** Build a context from raw Zeebe variables or any string map. */
    public static WorkflowContext of(Map<String, Object> vars) {
        return new WorkflowContext(
            str(vars, "workflow_id",    UUID.randomUUID().toString()),
            str(vars, "tenant_id",      "default"),
            str(vars, "correlation_id", UUID.randomUUID().toString()),
            str(vars, "source",         DalSource.DB.name())
        );
    }

    public static WorkflowContext of(String workflowId, String tenantId,
                                     String correlationId, String source) {
        return new WorkflowContext(workflowId, tenantId, correlationId, source);
    }

    /** Install this context on the current thread and populate MDC. */
    public static void set(WorkflowContext ctx) {
        HOLDER.set(ctx);
        MDC.put(KEY_WORKFLOW_ID,    ctx.workflowId);
        MDC.put(KEY_TENANT_ID,      ctx.tenantId);
        MDC.put(KEY_CORRELATION_ID, ctx.correlationId);
        MDC.put(KEY_SOURCE,         ctx.source);
        if (ctx.step != null) MDC.put(KEY_STEP, ctx.step);
    }

    /** Returns the context for the current thread; never null in a properly instrumented worker. */
    public static WorkflowContext current() {
        WorkflowContext ctx = HOLDER.get();
        if (ctx == null) throw new IllegalStateException(
            "No WorkflowContext on current thread — call WorkflowContext.set() at the entry point.");
        return ctx;
    }

    /** Returns current context or null if not set (non-throwing). */
    public static WorkflowContext currentOrNull() {
        return HOLDER.get();
    }

    /** Remove from thread and clear MDC fields. Call in finally blocks. */
    public static void clear() {
        HOLDER.remove();
        MDC.remove(KEY_WORKFLOW_ID);
        MDC.remove(KEY_TENANT_ID);
        MDC.remove(KEY_CORRELATION_ID);
        MDC.remove(KEY_SOURCE);
        MDC.remove(KEY_STEP);
    }

    /** Update the current step label — reflected in MDC immediately. */
    public WorkflowContext atStep(String stepName) {
        this.step = stepName;
        MDC.put(KEY_STEP, stepName);
        return this;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────
    public String getWorkflowId()    { return workflowId; }
    public String getTenantId()      { return tenantId; }
    public String getCorrelationId() { return correlationId; }
    public String getSource()        { return source; }
    public String getStep()          { return step; }

    private static String str(Map<String, Object> m, String key, String fallback) {
        Object v = m.get(key);
        return (v instanceof String s && !s.isBlank()) ? s : fallback;
    }
}
