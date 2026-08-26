%% camunda_l3_worker — L3 Orchestration layer: Zeebe job worker behaviour.
%%
%% PURPOSE
%%   Defines the callback interface for Zeebe job workers and owns the
%%   gRPC long-poll setup. Services implement this behaviour and call
%%   camunda_l3_worker:start_link/3 from their supervisor.
%%
%% KEY INVARIANT
%%   Workers POLL Zeebe via gRPC long-poll. Zeebe does NOT push.
%%   The job type string in the BPMN (<zeebe:taskDefinition type="..."/>)
%%   must EXACTLY match the type passed to start_link/3. No other mapping.
%%
%% USAGE (in your service, e.g. pick_front_scan_worker.erl)
%%
%%   -module(pick_front_scan_worker).
%%   -behaviour(camunda_l3_worker).
%%
%%   -define(JOB_TYPE, <<"pick.front.process_scan">>).
%%
%%   start_link() ->
%%       camunda_l3_worker:start_link(?JOB_TYPE, ?MODULE, #{retries => 3}).
%%
%%   handle_job(#{variables := Vars} = Job) ->
%%       OrderId = maps:get(<<"order_id">>, Vars),
%%       case pick_front_saga:commit_scan(OrderId, ...) of
%%           ok    -> camunda_l3_worker:complete(Job, #{<<"result">> => <<"ok">>});
%%           Error -> camunda_l3_worker:fail(Job, Error, 2)
%%       end.

-module(camunda_l3_worker).
-include_lib("butler_shared/include/tracing.hrl").

%% ── Behaviour callbacks ──────────────────────────────────────────
-callback handle_job(map()) -> ok | {error, term()}.

%% ── API ──────────────────────────────────────────────────────────
-export([start_link/3, complete/2, fail/3]).

%% Starts a Zeebe worker that polls for jobs of the given type.
%% Opts: #{retries => pos_integer(), timeout_ms => pos_integer()}
start_link(JobType, HandlerMod, Opts) ->
    zeebe_worker:start_link(JobType, HandlerMod, Opts).

%% Complete a job successfully, passing output variables back to Zeebe.
complete(Job, OutVars) ->
    zeebe_worker:complete(Job, OutVars).

%% Fail a job; RemainingRetries tells Zeebe how many retries are left.
fail(Job, Reason, RemainingRetries) ->
    zeebe_worker:fail(Job, #{
        error_message   => format_reason(Reason),
        retries         => RemainingRetries
    }).

%% ── Internal ─────────────────────────────────────────────────────

format_reason(Reason) when is_binary(Reason)  -> Reason;
format_reason(Reason) when is_atom(Reason)    -> atom_to_binary(Reason);
format_reason(Reason)                         -> iolist_to_binary(io_lib:format("~p", [Reason])).
