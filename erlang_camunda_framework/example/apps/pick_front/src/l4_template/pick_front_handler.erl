%% L4 example — Cowboy handler using camunda_l4_handler behaviour.

-module(pick_front_handler).
-behaviour(camunda_l4_handler).

-export([init/2, handle_get/2, handle_post/2]).

init(Req, State) ->
    camunda_l4_handler:init(?MODULE, Req, State).

handle_get(Req, State) ->
    OrderId = cowboy_req:binding(order_id, Req),
    case pick_front_db:get(OrderId) of
        {ok, Rec}          -> {ok, pick_front_view:to_map(Rec), Req, State};
        {error, not_found} -> {not_found, Req, State}
    end.

handle_post(Req, State) ->
    Params     = camunda_l4_handler:read_json(Req),
    OrderId    = maps:get(<<"order_id">>,    Params),
    PpsId      = maps:get(<<"pps_id">>,      Params),
    ScanResult = maps:get(<<"scan_result">>, Params, <<>>),
    case pick_front_saga:commit_scan(OrderId, PpsId, ScanResult) of
        ok    -> {ok, #{result => <<"ok">>, order_id => OrderId}, Req, State};
        Error -> {error, Error, Req, State}
    end.
