package com.greyorange.camunda.l2;

/**
 * L2 · Rule Engine — single business rule.
 *
 * Child saga classes define their invariants as WorkflowRule instances
 * and pass them to WorkflowSagaBase.runSaga() for enforcement before commit.
 * This keeps rule definitions in the child (domain knowledge) while the
 * enforcement mechanism lives in the base (framework).
 *
 * TYPE PARAM:
 *   C — the context object the rule evaluates (entity, DTO, command, etc.)
 *
 * USAGE (define rules in the child saga):
 *
 *   private final WorkflowRule<PickFrontEntity> BIN_ASSIGNED = WorkflowRule.of(
 *       "bin_must_be_assigned",
 *       "Order must have a bin assigned before scan is committed",
 *       entity -> entity.getBinId() != null
 *   );
 *
 *   // pass to runSaga:
 *   runSaga(ctx, List.of(BIN_ASSIGNED, ORDER_IN_CREATED_STATE), commit, "pick.events");
 *
 * VIOLATION:
 *   If evaluate() returns false, WorkflowRuleEngine throws RuleViolationException
 *   before the commit lambda is called — no DB write occurs.
 */
public interface WorkflowRule<C> {

    /** Short machine-readable identifier. Used in exception messages and metrics. */
    String name();

    /** Human-readable description of what this rule enforces. */
    String description();

    /**
     * Evaluate the rule against the given context.
     * Must be side-effect-free — never write to DB or send events here.
     *
     * @return true if the rule passes; false if violated
     */
    boolean evaluate(C context);

    /** Convenience factory to create a rule from a lambda. */
    static <C> WorkflowRule<C> of(String name, String description,
                                   java.util.function.Predicate<C> predicate) {
        return new WorkflowRule<>() {
            @Override public String name()              { return name; }
            @Override public String description()       { return description; }
            @Override public boolean evaluate(C context) { return predicate.test(context); }
        };
    }
}
