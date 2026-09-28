package com.greyorange.camunda.l3;

import com.greyorange.camunda.l2.WorkflowRule;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import io.camunda.connector.api.outbound.OutboundConnectorProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * L3 - base for an {@code @OutboundConnector} exposing several
 * {@code @Operation} methods. Every real operation in this framework follows
 * the same four-step shape: enforce guardrails, call out (butler_server, any
 * other system), persist a phase transition through the L2 saga, merge both
 * results into the job's output variables. This class turns that repeated
 * shape into one call instead of hand-rolling it in every method.
 *
 * What it deliberately does NOT abstract: which guardrails apply, what the
 * external call actually asks, and which phase it persists to -- that's the
 * real business logic and is different for every operation. Camunda's
 * Connector SDK already handles job polling/completion/retries on its own
 * (implementing {@link OutboundConnectorProvider} is enough for that), so
 * unlike the old {@code AbstractJobWorker}, this class has nothing to do with
 * job completion machinery -- only with cutting the repeated boilerplate
 * inside each operation method.
 */
public abstract class AbstractOperationConnector implements OutboundConnectorProvider {

    protected final WorkflowRuleEngine ruleEngine;

    protected AbstractOperationConnector(WorkflowRuleEngine ruleEngine) {
        this.ruleEngine = ruleEngine;
    }

    /**
     * Runs one operation's standard shape and returns the merged result map.
     *
     * @param ruleContext  object guardrails are evaluated against (e.g. the ppsId)
     * @param guards       rules to enforce before doing anything; empty list skips enforcement
     * @param externalCall supplies whatever the operation actually asked an external system
     * @param persistPhase supplies the L2 saga's own result of persisting the phase transition
     * @param <C>          rule context type
     */
    protected <C> Map<String, Object> runOperation(
        C ruleContext,
        List<WorkflowRule<C>> guards,
        Supplier<Map<String, Object>> externalCall,
        Supplier<Map<String, Object>> persistPhase
    ) {
        ruleEngine.enforceAll(ruleContext, guards);
        Map<String, Object> externalResult = externalCall.get();
        Map<String, Object> phaseResult = persistPhase.get();
        Map<String, Object> merged = new HashMap<>(externalResult);
        merged.putAll(phaseResult);
        return merged;
    }
}
