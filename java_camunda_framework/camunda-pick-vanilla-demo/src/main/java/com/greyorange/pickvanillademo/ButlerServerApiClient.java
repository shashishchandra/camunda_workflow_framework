package com.greyorange.pickvanillademo;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

/**
 * Thin REST client over butler_server's pick_vanilla_demo_http_handler.
 * Every method here wraps exactly one read-only endpoint from that handler
 * -- no business logic lives here, and there is no write method anywhere
 * in this class. See that handler's moduledoc for which real Erlang
 * function each endpoint wraps.
 */
@Component
public class ButlerServerApiClient {

    private static final String BASE_PATH = "/api/pick/v1/pickVanillaDemo";
    private static final ParameterizedTypeReference<Map<String, Object>> MAP_TYPE =
        new ParameterizedTypeReference<>() {};

    private final RestClient restClient;

    public ButlerServerApiClient(RestClient butlerServerRestClient) {
        this.restClient = butlerServerRestClient;
    }

    /** Wraps GET hasClearableFrontBin (priority_bin_hsm_utils:has_clearable_front_bin/1). */
    public boolean hasClearableFrontBin(String ppsId) {
        return getBooleanResult("/hasClearableFrontBin", ppsId);
    }

    /** Wraps GET isFrontToteFlow (pick_front_fsm_utils:is_front_tote_flow/1). */
    public boolean isFrontToteFlow(String ppsId) {
        return getBooleanResult("/isFrontToteFlow", ppsId);
    }

    /** Wraps GET isDestinationOrchestratedByHtm (ppsnode:is_destination_orchestrated_by_htm/1). */
    public boolean isDestinationOrchestratedByHtm(String ppsId) {
        return getBooleanResult("/isDestinationOrchestratedByHtm", ppsId);
    }

    /** Wraps GET printAwaited (dock_station_info:get_dock_station_id_by_status/2). Empty when none awaited. */
    @SuppressWarnings("unchecked")
    public List<String> printAwaitedDockStationIds(String ppsId) {
        Map<String, Object> body = restClient.get()
            .uri(BASE_PATH + "/printAwaited?ppsId={ppsId}", ppsId)
            .retrieve()
            .body(MAP_TYPE);
        return (List<String>) body.getOrDefault("dock_station_ids", List.of());
    }

    /** Wraps POST isPickPossible (priority_bin_hsm_utils:is_pick_possible/2). */
    public boolean isPickPossible(String ppsId, List<Map<String, Object>> pickList) {
        Map<String, Object> body = restClient.post()
            .uri(BASE_PATH + "/isPickPossible")
            .body(Map.of("ppsId", Integer.parseInt(ppsId), "pickList", pickList))
            .retrieve()
            .body(MAP_TYPE);
        return (Boolean) body.get("result");
    }

    /** Wraps GET rackDetails (rackref:get_by_id/1) -- a debug-formatted string, not a typed projection. */
    public String rackDetails(String rackId) {
        return lookupData("/rackDetails?rackId={rackId}", rackId);
    }

    /** Wraps GET ppsBinDetails (ppsbinrec:get_by_id/1). */
    public String ppsBinDetails(String ppsId, String binId) {
        Map<String, Object> body = restClient.get()
            .uri(BASE_PATH + "/ppsBinDetails?ppsId={ppsId}&binId={binId}", ppsId, binId)
            .retrieve()
            .body(MAP_TYPE);
        return foundOrNotFound(body);
    }

    /** Wraps GET ppsDetails (ppsnode:get_by_id/1). */
    public String ppsDetails(String ppsId) {
        return lookupData("/ppsDetails?ppsId={ppsId}", ppsId);
    }

    /** Wraps GET config (pick_flow_configs:config_value/2, allow-listed keys only). */
    public String config(String key) {
        Map<String, Object> body = restClient.get()
            .uri(BASE_PATH + "/config?key={key}", key)
            .retrieve()
            .body(MAP_TYPE);
        return String.valueOf(body.getOrDefault("value", "<not allow-listed>"));
    }

    private String lookupData(String pathTemplate, String param) {
        Map<String, Object> body = restClient.get()
            .uri(BASE_PATH + pathTemplate, param)
            .retrieve()
            .body(MAP_TYPE);
        return foundOrNotFound(body);
    }

    private String foundOrNotFound(Map<String, Object> body) {
        if (Boolean.TRUE.equals(body.get("found"))) {
            return String.valueOf(body.get("data"));
        }
        return "<not found>";
    }

    private boolean getBooleanResult(String path, String ppsId) {
        Map<String, Object> body = restClient.get()
            .uri(BASE_PATH + path + "?ppsId={ppsId}", ppsId)
            .retrieve()
            .body(MAP_TYPE);
        return (Boolean) body.get("result");
    }
}
