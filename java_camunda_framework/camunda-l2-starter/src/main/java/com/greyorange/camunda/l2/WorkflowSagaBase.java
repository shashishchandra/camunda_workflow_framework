package com.greyorange.camunda.l2;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * L2 Integrity — abstract base for all service saga classes.
 *
 * PATTERN: @Transactional + afterCommit = gen_saga:promise in Erlang.
 * Kafka events fire ONLY after DB commit — never on rollback.
 *
 * USAGE: extend and call runSaga() from your service methods:
 *
 *   @Service
 *   public class PickFrontSaga extends WorkflowSagaBase {
 *
 *       @Transactional
 *       public Map<String,Object> commitScan(String orderId, String scan) {
 *           return runSaga(
 *               () -> {
 *                   var entity = repository.findById(orderId).orElseThrow();
 *                   entity.setScanResult(scan);
 *                   entity.setStatus(WorkflowStatus.COMPLETED);
 *                   repository.save(entity);
 *                   // return value → Kafka event payload
 *                   return Map.of("type", "pick.front.scanned",
 *                                 "order_id", orderId,
 *                                 "scan", scan);
 *               },
 *               "pick.events"
 *           );
 *       }
 *   }
 *
 * RULES:
 *   - The commit lambda must be called inside a @Transactional method.
 *   - Never call kafka.send() directly in the lambda — use runSaga().
 *   - afterCommit fires only if the enclosing @Transactional commits.
 */
public abstract class WorkflowSagaBase {

    private final KafkaTemplate<String, Object> kafka;

    protected WorkflowSagaBase(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    /**
     * Run a transactional commit and publish the returned event to Kafka after commit.
     * Must be called from within a @Transactional method.
     */
    protected <T> T runSaga(SagaCommit<T> commit, String kafkaTopic) {
        T result = commit.execute();
        afterCommit(() -> kafka.send(kafkaTopic, result));
        return result;
    }

    /**
     * Run a transactional commit with a custom after-commit action
     * (e.g. Zeebe message publish + Kafka).
     */
    protected <T> T runSaga(SagaCommit<T> commit, String kafkaTopic, Runnable extraAfterCommit) {
        T result = commit.execute();
        afterCommit(() -> {
            kafka.send(kafkaTopic, result);
            extraAfterCommit.run();
        });
        return result;
    }

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

    @FunctionalInterface
    public interface SagaCommit<T> {
        T execute();
    }
}
