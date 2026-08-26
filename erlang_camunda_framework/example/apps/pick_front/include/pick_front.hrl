%% pick_front.hrl — example service using camunda_l1..l4 libraries.
%% Replace with your own service record + macros.

-record(pick_front_entity, {
    id         :: binary(),
    order_id   :: binary(),
    pps_id     :: binary(),
    status     :: pending | scanned | committed | sidelined,
    scan_result :: binary() | undefined,
    created_at :: integer(),
    updated_at :: integer()
}).

-define(PICK_FRONT_TABLE, pick_front_entity).
-define(KAFKA_TOPIC, <<"pick.events">>).
