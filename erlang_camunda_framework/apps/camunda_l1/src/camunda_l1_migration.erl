%% camunda_l1_migration — migration runner for Camunda-backed services.
%%
%% PURPOSE
%%   Runs up/down migrations for tables registered by L1 modules.
%%   Services register their migration module; this runner calls up/0 or
%%   down/0 in the correct butler_server ordering.
%%
%% USAGE
%%   camunda_l1_migration:run(up,   [pick_front_migration, put_migration]).
%%   camunda_l1_migration:run(down, [put_migration, pick_front_migration]).
%%
%% The caller is responsible for correct ordering (see butler_server
%% downgrade: GMR→GMC→GM Base→Base; upgrade is reverse).

-module(camunda_l1_migration).

-export([run/2, run_one/2]).

run(Direction, Modules) when Direction =:= up; Direction =:= down ->
    lists:foreach(fun(Mod) -> run_one(Direction, Mod) end, Modules).

run_one(up, Mod) ->
    case Mod:up() of
        ok             -> ok;
        {aborted, Rsn} -> error({migration_failed, up, Mod, Rsn})
    end;
run_one(down, Mod) ->
    case Mod:down() of
        ok             -> ok;
        {aborted, Rsn} -> error({migration_failed, down, Mod, Rsn})
    end.
