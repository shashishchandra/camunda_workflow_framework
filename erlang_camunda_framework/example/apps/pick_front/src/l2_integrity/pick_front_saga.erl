%% L2 example — uses camunda_l2_saga for pick_front state transitions.

-module(pick_front_saga).
-include("pick_front.hrl").

-export([commit_scan/3, sideline/2]).

commit_scan(OrderId, PpsId, ScanResult) ->
    camunda_l2_saga:run(
        fun() ->
            {ok, _} = pick_front_db:update(OrderId, #{
                status      => scanned,
                scan_result => ScanResult
            }),
            #{type       => <<"pick.front.scanned">>,
              order_id   => OrderId,
              pps_id     => PpsId,
              scan_result => ScanResult}
        end,
        ?KAFKA_TOPIC
    ).

sideline(OrderId, Reason) ->
    camunda_l2_saga:run(
        fun() ->
            {ok, _} = pick_front_db:update(OrderId, #{status => sidelined}),
            #{type => <<"pick.front.sidelined">>, order_id => OrderId,
              reason => Reason}
        end,
        ?KAFKA_TOPIC
    ).
