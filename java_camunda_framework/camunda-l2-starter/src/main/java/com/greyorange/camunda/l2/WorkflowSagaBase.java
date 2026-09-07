package com.greyorange.camunda.l2;

import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * L2 Integrity — abstract base for all workflow saga classes.
 *
 * Models the gen_saga:promise contract from the Erlang L2 layer:
 *   1. Rules are evaluated BEFORE the DB transaction opens — violated rules never write.
 *   2. Business commit runs inside the caller's @Transactional boundary.
 *   3. MQ event fires ONLY after DB commit — never on rollback.
 *   4. Checkpoints are persisted inside the same transaction as the business commit.
 *   5. Rollback hooks run synchronously when the transaction rolls back.
 *
 * DESIGN DECISIONS:
 *   - WorkflowMq replaces KafkaTemplate so the saga is MQ-agnostic.
 *   - WorkflowRuleEngine is injected (not created per-call) so it can be mocked in tests.
 *   - EntityManager is injected for checkpoint persistence without requiring a
 *     full repository dependency in child classes.
 *   - onRollback() is a protected hook — override in children to emit compensation events,
 *     update status to FAILED, etc. Default is a no-op.
 *
 * USAGE:
 *
 *   @Service
 *   public class PickFrontSaga extends WorkflowSagaBase {
 *
 *       private static final WorkflowRule<PickFrontEntity> BIN_ASSIGNED = WorkflowRule.of(
 *           "bin_must_be_assigned",
 *           "Order must have a bin assigned before scan is committed",
 *           entity -> entity.getBinId() != null
 *       );
 *
 *       @Transactional
 *       public Map<String, Object> commitScan(String orderId, String scan) {
 *           var entity = repository.findById(orderId).orElseThrow();
 *           entity.setScanResult(scan);
 *           entity.setStatus(WorkflowStatus.IN_PROGRESS);
 *
 *           return runSaga(
 *               entity,
 *               List.of(BIN_ASSIGNED),
 *               () -> {
 *                   entity.setStatus(WorkflowStatus.COMPLETED);
 *                   repository.save(entity);
 *                   return Map.of("type", "pick.front.scanned", "order_id", orderId);
 *               },
 *               "pick.events",
 *               "Task_ScanBarcode"   // checkpoint step name — matches BPMN task ID
 *           );
 *       }
 *
 *       @Override
 *       protected void onRollback(Throwable cause) {
 *           log.error("PickFront saga rolled back", cause);
 *           // optionally publish compensation event via mq
 *       }
 *   }
 */
public abstract class WorkflowSagaBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSagaBase.class);

    private final WorkflowMq mq;
    private final WorkflowRuleEngine ruleEngine;
    private final EntityManager em;

    protected WorkflowSagaBase(WorkflowMq mq, WorkflowRuleEngine ruleEngine, EntityManager em) {
        this.mq          = mq;
        this.ruleEngine  = ruleEngine;
        this.em          = em;
    }

    // ─── Primary runSaga ───────────────────────────────────────────────────────

    /**
     * Full saga run: enforce rules → commit → checkpoint → publish after commit.
     *
     * @param ruleContext    object to evaluate rules against (entity, command, etc.)
     * @param rules          rules to enforce before commit; empty list skips enforcement
     * @param commit         lambda that writes to DB and returns the MQ payload
     * @param destination    MQ destination (topic, queue, etc.) for the post-commit event
     * @param checkpointStep named step for the checkpoint row; null to skip checkpointing
     * @param <C>            rule context type
     * @param <T>            commit return / MQ payload type
     * @return               the value returned by the commit lambda
     * @throws RuleViolationException if any rule fails (no DB write occurs)
     */
    protected <C, T> T runSaga(C ruleContext,
                                List<WorkflowRule<C>> rules,
                                SagaCommit<T> commit,
                                String destination,
                                String checkpointStep) {
        ruleEngine.enforceAll(ruleContext, rules);

        T result;
        try {
            result = commit.execute();
        } catch (Exception e) {
            registerRollbackCallback(e);
            throw e;
        }

        if (checkpointStep != null) {
            saveCheckpoint(checkpointStep, Collections.emptyMap());
        }

        afterCommit(() -> mq.publish(destination, result));
        return result;
    }

    /**
     * Saga run without rules — commits and publishes without pre-flight enforcement.
     * Use when the caller guarantees context validity (e.g. idempotency replay path).
     */
    protected <T> T runSaga(SagaCommit<T> commit, String destination) {
        return runSaga(null, Collections.emptyList(), commit, destination, null);
    }

    /**
     * Saga run without rules, with checkpoint.
     */
    protected <T> T runSaga(SagaCommit<T> commit, String destination, String checkpointStep) {
        return runSaga(null, Collections.emptyList(), commit, destination, checkpointStep);
    }

    /**
     * Saga run with rules but no post-commit MQ event and no checkpoint.
     * Use for pure DB mutations that don't require downstream notification.
     */
    protected <C, T> T runSagaNoEvent(C ruleContext,
                                       List<WorkflowRule<C>> rules,
                                       SagaCommit<T> commit) {
        ruleEngine.enforceAll(ruleContext, rules);
        T result;
        try {
            result = commit.execute();
        } catch (Exception e) {
            registerRollbackCallback(e);
            throw e;
        }
        return result;
    }

    // ─── Checkpointing ────────────────────────────────────────────────────────

    /**
     * Persist a checkpoint for the current workflow inside the active transaction.
     * Rolls back with the transaction if the commit fails.
     *
     * workflowId and tenantId are read from WorkflowContext — must be set before calling.
     *
     * @param step      named BPMN step / task ID
     * @param variables snapshot of relevant workflow variables (serialised to JSON by caller)
     */
    protected void saveCheckpoint(String step, Map<String, Object> variables) {
        var ctx = com.greyorange.camunda.l1.WorkflowContext.currentOrNull();
        if (ctx == null) {
            log.warn("saveCheckpoint called without an active WorkflowContext — skipping");
            return;
        }
        String json = toJson(variables);
        WorkflowCheckpoint checkpoint = new WorkflowCheckpoint(
            ctx.getWorkflowId(), ctx.getTenantId(), step, json
        );
        em.persist(checkpoint);
        log.debug("Checkpoint saved: workflowId={} step={}", ctx.getWorkflowId(), step);
    }

    // ─── Rollback hook ────────────────────────────────────────────────────────

    /**
     * Override to handle rollback — emit compensation events, update status to FAILED, etc.
     * Called synchronously when the enclosing @Transactional rolls back.
     * Do NOT write to the DB from here (the transaction is already rolling back).
     * Use a new @Transactional method (REQUIRES_NEW) if you need a compensation write.
     */
    protected void onRollback(Throwable cause) {
        // default: no-op
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    private void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            }
        );
    }

    private void registerRollbackCallback(Throwable cause) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    if (status == STATUS_ROLLED_BACK) {
                        try {
                            onRollback(cause);
                        } catch (Exception ex) {
                            log.error("onRollback hook threw — suppressing to preserve original cause", ex);
                        }
                    }
                }
            }
        );
    }

    /** Minimal JSON serialisation — replace with ObjectMapper injection in production. */
    private String toJson(Map<String, Object> variables) {
        if (variables == null || variables.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        variables.forEach((k, v) -> {
            if (sb.length() > 1) sb.append(",");
            sb.append("\"").append(k).append("\":");
            if (v instanceof String) sb.append("\"").append(v).append("\"");
            else sb.append(v);
        });
        sb.append("}");
        return sb.toString();
    }

    // ─── Functional interface ────────────────────────────────────────────────

    @FunctionalInterface
    public interface SagaCommit<T> {
        T execute();
    }
}
