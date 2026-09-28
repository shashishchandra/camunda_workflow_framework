package com.greyorange.camunda.l3;

import io.camunda.connector.api.inbound.Health;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.api.inbound.InboundConnectorExecutable;

/**
 * L3 - base for any {@code @InboundConnector} driven by an external event
 * stream, regardless of transport. Concrete connectors supply a
 * {@link MessageSource} (Kafka today; an MQ or an inbound HTTP trigger are
 * drop-in replacements later, see {@link MessageSource}) and only implement
 * {@link #onMessage}.
 *
 * Lifecycle is delegated straight to the source: {@link #activate} starts it,
 * {@link #deactivate} stops it, exactly matching when the connector runtime
 * wants this subscription alive.
 */
public abstract class AbstractEventInboundConnector implements InboundConnectorExecutable<InboundConnectorContext> {

    private final MessageSource messageSource;

    protected AbstractEventInboundConnector(MessageSource messageSource) {
        this.messageSource = messageSource;
    }

    /** Handle one raw message body against the currently active connector context. */
    protected abstract void onMessage(String rawMessage, InboundConnectorContext context);

    @Override
    public void activate(InboundConnectorContext context) {
        messageSource.start(raw -> onMessage(raw, context));
        context.reportHealth(Health.up());
    }

    @Override
    public void deactivate() {
        messageSource.stop();
    }
}
