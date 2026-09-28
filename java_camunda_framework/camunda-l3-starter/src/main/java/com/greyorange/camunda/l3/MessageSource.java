package com.greyorange.camunda.l3;

import java.util.function.Consumer;

/**
 * L3 - transport abstraction for anything that can hand a raw inbound
 * connector a stream of messages: Kafka today, an MQ or a plain inbound HTTP
 * trigger tomorrow, without changing the connector class itself.
 *
 * A connector depends on this interface, never on a concrete transport client
 * directly -- which transport a given connector actually uses becomes a Spring
 * wiring choice (which bean gets injected), not a code change.
 */
public interface MessageSource {

    /**
     * Start delivering messages. Every raw message body is handed to
     * {@code onRawMessage} as it arrives. Must be safe to call exactly once
     * per activation.
     */
    void start(Consumer<String> onRawMessage);

    /** Stop delivering messages and release any underlying resources. */
    void stop();
}
