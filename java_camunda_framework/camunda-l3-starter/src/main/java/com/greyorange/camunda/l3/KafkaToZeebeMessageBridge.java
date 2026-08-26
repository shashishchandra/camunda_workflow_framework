package com.greyorange.camunda.l3;

import io.camunda.zeebe.client.ZeebeClient;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

/**
 * L3 — bridges Kafka events to Zeebe intermediate message events (IWE).
 *
 * Use this when a BPMN process waits on an external event (e.g. rack arrived,
 * carrier dispatched) that arrives via Kafka. The Zeebe message name must match
 * the <bpmn:message name="..."/> element in the BPMN exactly.
 *
 * USAGE (in your Kafka consumer):
 *
 *   @KafkaListener(topics = "rack.events")
 *   public void onRackEvent(RackEvent event) {
 *       if ("rack.arrived".equals(event.getType())) {
 *           bridge.sendMessage(
 *               "Msg_RackArrived",        // must match BPMN message name
 *               event.getPpsId(),          // correlation key
 *               Map.of("rack_id", event.getRackId())
 *           );
 *       }
 *   }
 */
@Component
public class KafkaToZeebeMessageBridge {

    private final ZeebeClient zeebeClient;

    public KafkaToZeebeMessageBridge(ZeebeClient zeebeClient) {
        this.zeebeClient = zeebeClient;
    }

    /** Send a Zeebe message with no TTL (uses Zeebe cluster default). */
    public void sendMessage(String messageName, String correlationKey, Map<String, Object> variables) {
        zeebeClient.newPublishMessageCommand()
                   .messageName(messageName)
                   .correlationKey(correlationKey)
                   .variables(variables)
                   .send()
                   .join();
    }

    /** Send a Zeebe message with explicit TTL. */
    public void sendMessage(String messageName, String correlationKey,
                            Map<String, Object> variables, Duration ttl) {
        zeebeClient.newPublishMessageCommand()
                   .messageName(messageName)
                   .correlationKey(correlationKey)
                   .variables(variables)
                   .timeToLive(ttl)
                   .send()
                   .join();
    }
}
