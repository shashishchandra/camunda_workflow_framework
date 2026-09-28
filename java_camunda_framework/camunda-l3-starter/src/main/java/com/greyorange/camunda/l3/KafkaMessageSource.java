package com.greyorange.camunda.l3;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * L3 - a {@link MessageSource} backed by a raw {@link KafkaConsumer}, owned and
 * polled on its own daemon thread for the lifetime of one connector activation.
 *
 * Deliberately NOT built on Spring's {@code @KafkaListener}: inbound connector
 * beans are prototype-scoped (required by the Connector SDK) and instantiated
 * on demand by the connector runtime, well after the application context has
 * finished its normal startup -- Spring's declarative listener wiring does not
 * reliably pick up annotations on a bean created that late. Owning the consumer
 * directly, started in {@code start()} and stopped in {@code stop()}, sidesteps
 * that entirely and mirrors exactly when the connector runtime wants it alive.
 */
public class KafkaMessageSource implements MessageSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageSource.class);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private volatile KafkaConsumer<String, String> consumer;
    private volatile Thread pollThread;

    public KafkaMessageSource(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public void start(Consumer<String> onRawMessage) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> newConsumer = new KafkaConsumer<>(props);
        newConsumer.subscribe(Collections.singletonList(topic));
        this.consumer = newConsumer;

        Thread thread = new Thread(() -> pollLoop(onRawMessage), "kafka-source-" + topic + "-poll");
        thread.setDaemon(true);
        this.pollThread = thread;
        thread.start();
    }

    private void pollLoop(Consumer<String> onRawMessage) {
        KafkaConsumer<String, String> activeConsumer = this.consumer;
        try {
            while (true) {
                ConsumerRecords<String, String> records = activeConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        onRawMessage.accept(record.value());
                    } catch (Exception e) {
                        log.warn("Failed to handle message on topic {}: {}", topic, e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            log.debug("Poll loop stopping for topic {}", topic);
        } finally {
            activeConsumer.close();
        }
    }

    @Override
    public void stop() {
        KafkaConsumer<String, String> activeConsumer = this.consumer;
        Thread thread = this.pollThread;
        if (activeConsumer != null) {
            activeConsumer.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        this.consumer = null;
        this.pollThread = null;
    }
}
