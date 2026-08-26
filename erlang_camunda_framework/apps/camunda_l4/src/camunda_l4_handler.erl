%% camunda_l4_handler — L4 Template layer: Cowboy REST handler behaviour.
%%
%% PURPOSE
%%   Enforces the three-layer Cowboy pattern across all services:
%%     Layer A: routing + auth guard (this module / the behaviour)
%%     Layer B: delegate to L2 saga — no business logic in handlers
%%     Layer C: serialise response via a view module
%%
%% Services implement the -callback handle_get/2 and handle_post/2 callbacks.
%% This module owns content negotiation, 404/422 error responses, and
%% JSON serialisation boilerplate.
%%
%% USAGE (in your service, e.g. pick_front_handler.erl)
%%
%%   -module(pick_front_handler).
%%   -behaviour(camunda_l4_handler).
%%   -export([init/2, handle_get/2, handle_post/2]).
%%
%%   init(Req, State) ->
%%       camunda_l4_handler:init(?MODULE, Req, State).
%%
%%   handle_get(Req, State) ->
%%       Id = cowboy_req:binding(id, Req),
%%       case pick_front_db:get(Id) of
%%           {ok, Rec} -> {ok, pick_front_view:to_map(Rec), Req, State};
%%           {error, not_found} -> {not_found, Req, State}
%%       end.
%%
%%   handle_post(Req, State) ->
%%       Params = camunda_l4_handler:read_json(Req),
%%       case pick_front_saga:commit(Params) of
%%           ok    -> {ok, #{result => <<"ok">>}, Req, State};
%%           Error -> {error, Error, Req, State}
%%       end.

-module(camunda_l4_handler).
-behaviour(cowboy_rest).

%% ── Behaviour callbacks ──────────────────────────────────────────
-callback handle_get(Req :: cowboy_req:req(), State :: term()) ->
    {ok, map(), cowboy_req:req(), term()} |
    {not_found, cowboy_req:req(), term()}.

-callback handle_post(Req :: cowboy_req:req(), State :: term()) ->
    {ok, map(), cowboy_req:req(), term()} |
    {error, term(), cowboy_req:req(), term()}.

%% ── Cowboy entry ─────────────────────────────────────────────────
-export([init/3, allowed_methods/2, content_types_provided/2,
         content_types_accepted/2, to_json/2, from_json/2]).

%% Services call this from their init/2 instead of {cowboy_rest, ...} directly.
init(Mod, Req, State) ->
    {cowboy_rest, Req, {Mod, State}}.

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"POST">>], Req, State}.

content_types_provided(Req, State) ->
    {[{<<"application/json">>, to_json}], Req, State}.

content_types_accepted(Req, State) ->
    {[{<<"application/json">>, from_json}], Req, State}.

to_json(Req, {Mod, Inner} = State) ->
    case Mod:handle_get(Req, Inner) of
        {ok, Body, Req2, Inner2} ->
            {jsx:encode(Body), Req2, {Mod, Inner2}};
        {not_found, Req2, Inner2} ->
            Req3 = cowboy_req:reply(404, #{}, <<>>, Req2),
            {stop, Req3, {Mod, Inner2}}
    end.

from_json(Req, {Mod, Inner} = State) ->
    case Mod:handle_post(Req, Inner) of
        {ok, Body, Req2, Inner2} ->
            Req3 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>},
                                    jsx:encode(Body), Req2),
            {true, Req3, {Mod, Inner2}};
        {error, Reason, Req2, Inner2} ->
            Req3 = cowboy_req:reply(422, #{}, jsx:encode(#{error => Reason}), Req2),
            {stop, Req3, {Mod, Inner2}}
    end.

%% ── Utilities ────────────────────────────────────────────────────
-export([read_json/1]).

read_json(Req) ->
    {ok, Body, _Req2} = cowboy_req:read_body(Req),
    jsx:decode(Body, [return_maps]).
