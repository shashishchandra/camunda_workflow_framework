package com.greyorange.pickvanillademo.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greyorange.camunda.l3.AbstractEventInboundConnector;
import com.greyorange.camunda.l3.KafkaMessageSource;
import com.greyorange.pickvanillademo.PickInstructionPhase;
import com.greyorange.pickvanillademo.PickInstructionSaga;
import io.camunda.connector.api.annotation.InboundConnector;
import io.camunda.connector.api.inbound.CorrelationRequest;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.generator.java.annotation.ElementTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo-triggered only: butler_server never publishes to this topic -- for the
 * demo, an operator scan ({@code scan} or {@code multiscan}, distinguished by
 * the payload's scan_type field) is simulated by hand-publishing a message
 * here. Wired to the BPMN's "Wait For Entity Scan" intermediate catch event.
 */
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@InboundConnector(name = "Wait For Entity Scan (DEMO event)", type = "io.greyorange.pickvanillademo:entity-scan:1")
@ElementTemplate(
    id = "io.greyorange.pickvanillademo.EntityScan.v1",
    name = "Wait For Entity Scan (DEMO event)",
    version = 1,
    description = "Waits for a hand-published DEMO Kafka message simulating an operator scan. "
        + "butler_server never publishes to this topic.")
public class EntityScanInboundConnector extends AbstractEventInboundConnector {

    private static final Logger log = LoggerFactory.getLogger(EntityScanInboundConnector.class);

    private final PickInstructionSaga saga;
    private final ObjectMapper objectMapper;

    public EntityScanInboundConnector(
        PickInstructionSaga saga,
        ObjectMapper objectMapper,
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        super(new KafkaMessageSource(bootstrapServers, "pick.vanilla_demo.scan", "pick-vanilla-demo-entity-scan"));
        this.saga = saga;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void onMessage(String rawJson, InboundConnectorContext ctx) {
        Map<String, Object> payload = parse(rawJson);
        if (payload == null) {
            return;
        }
        Object ppsIdField = payload.get("pps_id");
        if (ppsIdField == null) {
            log.warn("Ignoring demo scan event with no pps_id: {}", payload);
            return;
        }
        String ppsId = String.valueOf(ppsIdField);
        Map<String, Object> sagaVars = saga.advancePhase(
            ppsId, PickInstructionPhase.WAIT_FOR_ENTITY_SCAN, PickInstructionPhase.PICKLIST_PROCESSING, "Op_WaitForEntityScan");
        Map<String, Object> variables = new HashMap<>(payload);
        variables.putAll(sagaVars);
        ctx.correlate(CorrelationRequest.builder().variables(variables).build());
    }

    private Map<String, Object> parse(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("Discarding malformed scan message: {}", e.getMessage());
            return null;
        }
    }
}
