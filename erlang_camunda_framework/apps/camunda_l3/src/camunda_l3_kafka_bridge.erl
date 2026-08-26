%% camunda_l3_kafka_bridge — Kafka event → Zeebe message bridge.
%%
%% PURPOSE
%%   Many IWE (Intermediate Wait Event) patterns require a Zeebe message
%%   to be published when an external event arrives (e.g. rack_arrived on
%%   Kafka → Msg_RackArrived on Zeebe). This module provides the plumbing.
%%
%% USAGE (in your service Kafka consumer, e.g. pick_front_kafka.erl)
%%
%%   handle_message(#{type := <<"rack.arrived">>,
%%                    rack_id := RackId, pps_id := PpsId}) ->
%%       camunda_l3_kafka_bridge:send_zeebe_message(
%%           <<"Msg_RackArrived">>,   %% must match correlateWith in BPMN
%%           PpsId,                   %% correlation key
%%           #{rack_id => RackId}     %% variables merged into process
%%       );
%%   handle_message(_) -> ok.
%%
%% RULES
%%   - Message name must EXACTLY match the message name in the BPMN
%%     <bpmn:message name="..."/> element.
%%   - Correlation key must match the subscriptionKey expression in BPMN.

-module(camunda_l3_kafka_bridge).

-export([send_zeebe_message/3, send_zeebe_message/4]).

%% send_zeebe_message/3 — no TTL (message expires after Zeebe default)
send_zeebe_message(MessageName, CorrelationKey, Variables) ->
    send_zeebe_message(MessageName, CorrelationKey, Variables, 0).

%% send_zeebe_message/4 — explicit TTL in milliseconds (0 = no TTL)
send_zeebe_message(MessageName, CorrelationKey, Variables, TtlMs) ->
    zeebe_client:publish_message(
        name            => MessageName,
        correlation_key => CorrelationKey,
        variables       => Variables,
        time_to_live    => TtlMs
    ).
