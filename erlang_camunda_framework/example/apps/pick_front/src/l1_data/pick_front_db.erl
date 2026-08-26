%% L1 example — implements camunda_l1_db behaviour for pick_front.
%% Any other service (put_front_db, station_db, …) follows this same pattern.

-module(pick_front_db).
-behaviour(camunda_l1_db).
-include("pick_front.hrl").
-include_lib("camunda_l1/include/camunda_l1.hrl").

-export([table_name/0, new/1, apply_update/3]).
%% Re-export generic CRUD for callers who don't want to pass the Mod arg.
-export([create/1, get/1, update/2, delete/1]).

table_name() -> ?PICK_FRONT_TABLE.

new(Fields) ->
    Now = erlang:system_time(millisecond),
    #pick_front_entity{
        id          = maps:get(id,       Fields),
        order_id    = maps:get(order_id, Fields),
        pps_id      = maps:get(pps_id,   Fields),
        status      = maps:get(status,   Fields, pending),
        scan_result = maps:get(scan_result, Fields, undefined),
        created_at  = Now,
        updated_at  = Now
    }.

apply_update(status,      V, R) -> R#pick_front_entity{status      = V};
apply_update(scan_result, V, R) -> R#pick_front_entity{scan_result = V};
apply_update(updated_at,  V, R) -> R#pick_front_entity{updated_at  = V};
apply_update(_,           _, R) -> R.

%% Convenience wrappers — delegates to camunda_l1_db generic impl.
create(Record)       -> camunda_l1_db:create(?MODULE, Record).
get(Id)              -> camunda_l1_db:get(?MODULE, Id).
update(Id, Updates)  -> camunda_l1_db:update(?MODULE, Id, Updates).
delete(Id)           -> camunda_l1_db:delete(?MODULE, Id).
