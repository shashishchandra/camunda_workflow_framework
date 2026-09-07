package com.greyorange.camunda.l2;

/**
 * L2 · Generic Message Queue abstraction.
 *
 * Replaces hard-coded KafkaTemplate so WorkflowSagaBase is MQ-agnostic.
 * Implement this interface for your broker of choice and inject it into the saga.
 *
 * PROVIDED IMPLEMENTATIONS (in separate modules):
 *   KafkaMq   — wraps KafkaTemplate<String, Object>
 *   RabbitMq  — wraps RabbitTemplate
 *   SqsMq     — wraps SqsAsyncClient
 *   NoopMq    — for tests / flows with no downstream event
 *
 * USAGE IN SAGA:
 *   WorkflowSagaBase calls mq.publish(destination, payload) inside afterCommit(),
 *   so the message is sent only after the DB transaction has committed successfully.
 *
 * EXAMPLE (Kafka implementation):
 *
 *   @Component
 *   public class KafkaMq implements WorkflowMq {
 *       private final KafkaTemplate<String, Object> kafka;
 *       @Override
 *       public void publish(String topic, Object payload) {
 *           kafka.send(topic, payload);
 *       }
 *   }
 */
public interface WorkflowMq {

    /**
     * Publish a message to the given destination (topic, queue, exchange, etc.).
     * Called after DB commit — implementations must not open a new DB transaction.
     *
     * @param destination broker-specific address (Kafka topic, RabbitMQ routing key, etc.)
     * @param payload     the event payload; must be serialisable by the broker client
     */
    void publish(String destination, Object payload);
}
