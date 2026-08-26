%% camunda_l1_db — L1 Data layer behaviour.
%%
%% PURPOSE
%%   Defines the interface every service-specific DB accessor module must
%%   implement. Provides default CRUD helpers that delegate to the callbacks.
%%   Services include camunda_l1 in rebar.config deps and implement this
%%   behaviour for their own Mnesia table.
%%
%% USAGE (in your service, e.g. pick_front_db.erl)
%%   -module(pick_front_db).
%%   -behaviour(camunda_l1_db).
%%   -include_lib("camunda_l1/include/camunda_l1.hrl").
%%   -export([table_name/0, new/1, apply_update/3]).
%%   table_name()           -> pick_front_entity.
%%   new(Fields)            -> #pick_front_entity{...}.
%%   apply_update(status,V,R) -> R#pick_front_entity{status=V};
%%   apply_update(_,    _,R) -> R.
%%
%% Then call camunda_l1_db:get(pick_front_db, Id) etc.

-module(camunda_l1_db).
-include("camunda_l1.hrl").

%% ── Behaviour callbacks ──────────────────────────────────────────
%% Implement these in your service-specific DB module.
-callback table_name() -> atom().
-callback new(map()) -> tuple().             %% build a new record from a map
-callback apply_update(atom(), term(), tuple()) -> tuple(). %% field patcher

%% ── Generic CRUD (call with your Mod as first arg) ───────────────
-export([create/2, get/2, update/3, delete/2]).

create(Mod, Record) ->
    ?CAMUNDA_TRANSACTION(mnesia:write(Record)),
    ok.

get(Mod, Id) ->
    Table = Mod:table_name(),
    ?CAMUNDA_TRANSACTION(
        case mnesia:read({Table, Id}) of
            [Rec] -> {ok, Rec};
            []    -> {error, not_found}
        end
    ).

update(Mod, Id, Updates) ->
    ?CAMUNDA_TRANSACTION(
        case mnesia:read({Mod:table_name(), Id}) of
            [Rec] ->
                Now     = erlang:system_time(millisecond),
                Updated = maps:fold(
                    fun(K, V, R) -> Mod:apply_update(K, V, R) end,
                    Rec, maps:put(updated_at, Now, Updates)
                ),
                mnesia:write(Updated),
                {ok, Updated};
            [] ->
                {error, not_found}
        end
    ).

delete(Mod, Id) ->
    ?CAMUNDA_TRANSACTION(mnesia:delete({Mod:table_name(), Id})).
