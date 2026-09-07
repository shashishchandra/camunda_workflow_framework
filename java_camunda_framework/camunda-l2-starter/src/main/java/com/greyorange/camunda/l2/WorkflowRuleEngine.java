package com.greyorange.camunda.l2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * L2 · Rule Engine
 *
 * Evaluates a list of WorkflowRules against a context object and either
 * passes silently or throws RuleViolationException listing all violated rules.
 *
 * Called by WorkflowSagaBase.runSaga() BEFORE the commit lambda is invoked.
 * This ensures corrupt states can never reach the DB — exactly the gen_saga:promise
 * invariant from the Erlang L2 layer.
 *
 * DESIGN: runs ALL rules and collects ALL violations before throwing,
 * so the caller gets a complete picture in one exception, not just the first failure.
 *
 * USAGE (directly — if not using runSaga):
 *
 *   WorkflowRuleEngine engine = new WorkflowRuleEngine();
 *   engine.enforceAll(entity, List.of(BIN_ASSIGNED, ORDER_IN_CREATED_STATE));
 *   // throws RuleViolationException if any rule fails
 */
public class WorkflowRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRuleEngine.class);

    /**
     * Evaluate all rules against the context.
     * Throws RuleViolationException listing every violated rule if any fail.
     * Returns silently if all rules pass.
     *
     * @param context the object to evaluate rules against
     * @param rules   rules to run; empty list is a no-op
     * @throws RuleViolationException if one or more rules are violated
     */
    public <C> void enforceAll(C context, List<WorkflowRule<C>> rules) {
        if (rules == null || rules.isEmpty()) return;

        List<String> violations = new ArrayList<>();
        for (WorkflowRule<C> rule : rules) {
            try {
                if (!rule.evaluate(context)) {
                    log.warn("Rule violated: {} — {}", rule.name(), rule.description());
                    violations.add(rule.name() + ": " + rule.description());
                }
            } catch (Exception e) {
                log.error("Rule '{}' threw unexpectedly — treating as violation", rule.name(), e);
                violations.add(rule.name() + ": evaluation error — " + e.getMessage());
            }
        }

        if (!violations.isEmpty()) {
            throw new RuleViolationException(violations);
        }
    }

    /**
     * Evaluate rules and return the list of violated rule names (no throw).
     * Useful for dry-run validation or building UI-level error lists.
     */
    public <C> List<String> dryRun(C context, List<WorkflowRule<C>> rules) {
        if (rules == null || rules.isEmpty()) return List.of();
        List<String> violations = new ArrayList<>();
        for (WorkflowRule<C> rule : rules) {
            try {
                if (!rule.evaluate(context)) violations.add(rule.name());
            } catch (Exception e) {
                violations.add(rule.name());
            }
        }
        return violations;
    }
}
