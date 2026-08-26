%% L3 example — Zeebe worker using camunda_l3_worker behaviour.
%% Job type MUST match <zeebe:taskDefinition type="pick.front.process_scan"/>

-module(pick_front_scan_worker).
-behaviour(camunda_l3_worker).
-include_lib("butler_shared/include/tracing.hrl").

-define(JOB_TYPE, <<"pick.front.process_scan">>).

-export([start_link/0, handle_job/1]).

start_link() ->
    camunda_l3_worker:start_link(?JOB_TYPE, ?MODULE, #{retries => 3}).

handle_job(#{variables := Vars, key := JobKey} = Job) ->
    ?TRACE_SPAN(<<"pick_front.process_scan">>, #{job_key => JobKey}, fun() ->
        OrderId    = maps:get(<<"order_id">>,    Vars),
        PpsId      = maps:get(<<"pps_id">>,      Vars),
        ScanResult = maps:get(<<"scan_result">>, Vars, <<>>),
        case pick_front_saga:commit_scan(OrderId, PpsId, ScanResult) of
            ok ->
                camunda_l3_worker:complete(Job, #{
                    <<"scan_committed">> => true,
                    <<"order_id">>       => OrderId
                });
            {error, Reason} ->
                camunda_l3_worker:fail(Job, Reason, 2)
        end
    end).
