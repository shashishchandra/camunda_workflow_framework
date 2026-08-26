%% camunda_l1.hrl — L1 Data layer macros & types
%%
%% Include this in any service that uses camunda_l1_db behaviour.
%% All Mnesia writes MUST go through ?CAMUNDA_TRANSACTION.
%% This wraps butler_base ?MNESIA_TRANSACTION so that:
%%   1. Elvis lint catches direct mnesia:transaction/1 calls.
%%   2. The caller never needs to know the underlying macro source.

-ifndef(CAMUNDA_L1_HRL).
-define(CAMUNDA_L1_HRL, true).

-include_lib("butler_base/include/macros.hrl").

%% Re-export as the canonical macro name for this framework.
-define(CAMUNDA_TRANSACTION(Expr), ?MNESIA_TRANSACTION(Expr)).

%% Standard result types returned by all L1 accessors.
-type l1_ok()    :: ok | {ok, term()}.
-type l1_error() :: {error, not_found | term()}.
-type l1_result() :: l1_ok() | l1_error().

-endif.
