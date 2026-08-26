package com.greyorange.camunda.l4;

import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.Map;

/**
 * L4 Template — abstract base for WebSocket screen push.
 *
 * Screen type is driven by the ui_template process variable set by L3 workers,
 * NOT by BPMN task names. This means renaming a Camunda task in Modeler
 * never breaks the operator screen — only the variable value matters.
 *
 * USAGE: extend and inject into your L3 workers or L5 controllers:
 *
 *   @Component
 *   public class PickFrontScreenTemplate extends ScreenTemplateBase {
 *
 *       public PickFrontScreenTemplate(SimpMessagingTemplate ws) {
 *           super(ws, "/topic/screen");
 *       }
 *
 *       public void pushScanScreen(String ppsId, String orderId, String slot) {
 *           push(ppsId, "pick_scan", Map.of(
 *               "order_id",    orderId,
 *               "source_slot", slot
 *           ));
 *       }
 *   }
 *
 * WebSocket destination: /topic/screen/{stationId}
 * Payload: { "ui_template": "<name>", "variables": { ... } }
 */
public abstract class ScreenTemplateBase {

    private final SimpMessagingTemplate ws;
    private final String topicPrefix;

    protected ScreenTemplateBase(SimpMessagingTemplate ws, String topicPrefix) {
        this.ws = ws;
        this.topicPrefix = topicPrefix;
    }

    /**
     * Push a screen update to the operator UI at a specific station.
     *
     * @param stationId   PPS ID or station identifier — used as topic suffix
     * @param uiTemplate  screen name (e.g. "pick_scan", "put_confirm") —
     *                    must match a template registered in the operator UI
     * @param variables   process variables the screen needs to render
     */
    protected void push(String stationId, String uiTemplate, Map<String, Object> variables) {
        ws.convertAndSend(
            topicPrefix + "/" + stationId,
            Map.of(
                "ui_template", uiTemplate,
                "variables",   variables
            )
        );
    }

    /** Clear the screen (idle state) at a station. */
    protected void clear(String stationId) {
        ws.convertAndSend(
            topicPrefix + "/" + stationId,
            Map.of("ui_template", "idle", "variables", Map.of())
        );
    }
}
