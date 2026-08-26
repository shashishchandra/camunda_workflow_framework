%% L3 example — Kafka consumer + Zeebe message bridge using camunda_l3_kafka_bridge.

-module(pick_front_kafka).
-behaviour(butler_kafka_consumer).

-export([start_link/0, handle_message/1]).

start_link() ->
    butler_kafka_consumer:start_link(
        topic    => <<"rack.events">>,
        group_id => <<"pick_front">>,
        handler  => ?MODULE
    ).

handle_message(#{type := <<"rack.arrived">>, rack_id := RackId, pps_id := PpsId}) ->
    %% Bridge Kafka event → Zeebe IWE message.
    %% "Msg_RackArrived" must match <bpmn:message name="Msg_RackArrived"/> in BPMN.
    camunda_l3_kafka_bridge:send_zeebe_message(
        <<"Msg_RackArrived">>,
        PpsId,
        #{<<"rack_id">> => RackId, <<"pps_id">> => PpsId}
    );
handle_message(_Ignored) ->
    ok.
