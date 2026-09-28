package com.greyorange.pickvanillademo.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greyorange.camunda.l3.AbstractEventInboundConnector;
import com.greyorange.camunda.l3.KafkaMessageSource;
import com.greyorange.pickvanillademo.PickCyclePhase;
import com.greyorange.pickvanillademo.PickCycleSaga;
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
 * demo, a PPTL press is simulated by hand-publishing a message here carrying
 * bin_id/bin_side, which the outbound connector's commit_pptl_press and
 * auto_dest_clear operations read from process variables. Wired to the BPMN's
 * "Wait For Pick Bin Confirm" intermediate catch event.
 */
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@InboundConnector(name = "Wait For Pick Bin Confirm (DEMO event)", type = "io.greyorange.pickvanillademo:pick-bin-confirm:1")
@ElementTemplate(
    id = "io.greyorange.pickvanillademo.PickBinConfirm.v1",
    name = "Wait For Pick Bin Confirm (DEMO event)",
    version = 1,
    description = "Waits for a hand-published DEMO Kafka message simulating a PPTL button press. "
        + "butler_server never publishes to this topic.")
public class PickBinConfirmInboundConnector extends AbstractEventInboundConnector {

    private static final Logger log = LoggerFactory.getLogger(PickBinConfirmInboundConnector.class);

    private final PickCycleSaga saga;
    private final ObjectMapper objectMapper;

    public PickBinConfirmInboundConnector(
        PickCycleSaga saga,
        ObjectMapper objectMapper,
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        super(new KafkaMessageSource(bootstrapServers, "pick.vanilla_demo.pptl_press", "pick-vanilla-demo-pick-bin-confirm"));
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
            log.warn("Ignoring demo PPTL-press event with no pps_id: {}", payload);
            return;
        }
        String ppsId = String.valueOf(ppsIdField);
        Map<String, Object> sagaVars = saga.advancePhase(
            ppsId, PickCyclePhase.WAIT_FOR_PICK_BIN_CONFIRM, PickCyclePhase.WAIT_FOR_ENTITY_SCAN, "Op_WaitForPickBinConfirm");
        Map<String, Object> variables = new HashMap<>(payload);
        variables.putAll(sagaVars);
        ctx.correlate(CorrelationRequest.builder().variables(variables).build());
    }

    private Map<String, Object> parse(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("Discarding malformed PPTL-press message: {}", e.getMessage());
            return null;
        }
    }
}
