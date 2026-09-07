package com.greyorange.camunda.l2;

import java.util.List;

/**
 * L2 · Thrown when one or more WorkflowRules are violated before a saga commit.
 *
 * The DB transaction is NOT opened when this is thrown — no rollback needed.
 * The Zeebe worker should catch this and call client.newFailCommand() or
 * client.newThrowErrorCommand() to route the BPMN instance to an error boundary.
 */
public class RuleViolationException extends RuntimeException {

    private final List<String> violatedRules;

    public RuleViolationException(List<String> violatedRules) {
        super("Rule violation(s): " + String.join(", ", violatedRules));
        this.violatedRules = List.copyOf(violatedRules);
    }

    /** Names of the rules that evaluated to false. */
    public List<String> getViolatedRules() {
        return violatedRules;
    }
}
