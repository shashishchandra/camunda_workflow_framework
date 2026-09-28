package com.greyorange.pickvanillademo.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greyorange.camunda.l3.AbstractEventInboundConnector;
import com.greyorange.camunda.l3.KafkaMessageSource;
import com.greyorange.pickvanillademo.PickCycleSaga;
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
 * The ONE genuinely real Kafka hand-off in this demo. Consumes butler_server's
 * real, already-published rack-arrival topic on this app's own, independent
 * consumer group -- butler_server is not modified in any way and has no idea
 * this app exists. Only {@code status = "transport_complete"} starts a cycle;
 * every other status on this real topic (transport_restart, etc.) is ignored.
 *
 * Wired to the BPMN's start event via this connector's {@code type}; the
 * message correlation key ({@code =pps_id}) stays on the BPMN's own
 * {@code zeebe:subscription} -- that's a Zeebe-engine mechanism, independent of
 * which connector supplies the variables.
 */
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@InboundConnector(name = "Rack Arrival (REAL event)", type = "io.greyorange.pickvanillademo:rack-arrival:1")
@ElementTemplate(
    id = "io.greyorange.pickvanillademo.RackArrival.v1",
    name = "Rack Arrival (REAL event)",
    version = 1,
    description = "Starts a pick cycle from butler_server's real, already-published rack-arrival "
        + "topic (transport_request.workflow.pick.events), filtered to status=transport_complete.")
public class RackArrivalInboundConnector extends AbstractEventInboundConnector {

    private static final Logger log = LoggerFactory.getLogger(RackArrivalInboundConnector.class);

    private final PickCycleSaga saga;
    private final PickInstructionSaga instructionSaga;
    private final ObjectMapper objectMapper;

    public RackArrivalInboundConnector(
        PickCycleSaga saga,
        PickInstructionSaga instructionSaga,
        ObjectMapper objectMapper,
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        super(new KafkaMessageSource(bootstrapServers, "transport_request.workflow.pick.events", "pick-vanilla-demo-rack-arrival"));
        this.saga = saga;
        this.instructionSaga = instructionSaga;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void onMessage(String rawJson, InboundConnectorContext ctx) {
        Map<String, Object> payload = parse(rawJson);
        if (payload == null) {
            return;
        }
        if (!"transport_complete".equals(payload.get("status"))) {
            log.debug("Ignoring real rack-arrival event with status={} (only transport_complete starts the demo)",
                payload.get("status"));
            return;
        }
        Object ppsIdField = payload.get("pps_id");
        if (ppsIdField == null) {
            log.warn("Ignoring real rack-arrival event with no pps_id: {}", payload);
            return;
        }
        String ppsId = String.valueOf(ppsIdField);
        Map<String, Object> sagaVars = saga.waitForSourceArrival(ppsId);
        // First outer-loop iteration: the rack just arrived, so the first pick
        // instruction starts immediately. Later iterations are started by the
        // "any more pick instructions?" loop-back operation, not here.
        instructionSaga.startInstruction(ppsId);
        Map<String, Object> variables = new HashMap<>(payload);
        variables.putAll(sagaVars);
        ctx.correlate(CorrelationRequest.builder().variables(variables).build());
    }

    private Map<String, Object> parse(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("Discarding malformed rack-arrival message: {}", e.getMessage());
            return null;
        }
    }
}
