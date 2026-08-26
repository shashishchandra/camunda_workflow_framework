%% camunda_l2_saga — L2 Integrity layer: transactional write + Kafka publish.
%%
%% PURPOSE
%%   Thin wrapper around gen_saga:promise/2 with butler_kafka wired in.
%%   Services use this instead of calling gen_saga directly so that:
%%     1. The Kafka topic and event shape are enforced consistently.
%%     2. Kafka ALWAYS fires only after Mnesia commit — never on rollback.
%%        This is the Erlang equivalent of @Transactional + afterCommit.
%%
%% USAGE (in your service saga module, e.g. pick_front_saga.erl)
%%
%%   commit_scan(OrderId, ScanResult) ->
%%       camunda_l2_saga:run(
%%           fun() ->
%%               {ok, _} = pick_front_db:update(OrderId, #{scan => ScanResult}),
%%               %% return value → Kafka event payload
%%               #{type => <<"pick.front.scanned">>, order_id => OrderId}
%%           end,
%%           <<"pick.events">>   %% Kafka topic
%%       ).
%%
%% RULES
%%   - Phase 1 fun must return the Kafka event map on success.
%%   - Never call butler_kafka:publish/2 directly in Phase 1 — put it here.
%%   - Never put business routing logic in Phase 1 — that belongs in L3.

-module(camunda_l2_saga).
-include_lib("gen_saga/include/gen_saga.hrl").

-export([run/2, run/3]).

%% run/2 — publishes event returned by CommitFun to Topic.
run(CommitFun, Topic) ->
    gen_saga:promise(
        CommitFun,
        fun(Event) -> butler_kafka:publish(Topic, Event) end
    ).

%% run/3 — custom AfterCommitFun for side-effects beyond Kafka
%%          (e.g. sending a Zeebe message, pushing a WebSocket frame).
run(CommitFun, Topic, AfterCommitFun) ->
    gen_saga:promise(
        CommitFun,
        fun(Event) ->
            butler_kafka:publish(Topic, Event),
            AfterCommitFun(Event)
        end
    ).
