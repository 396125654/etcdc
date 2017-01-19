%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(etcd_client).

-behaviour(gen_server).

%% API
-export([ start_link/0
        , stop/0]).

-export([ watch_workernode_list/1
        , register_workernode/3
        , register_workernode/4
        , get_workernode/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(HIBERNATE_TIMEOUT, hibernate).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

-spec watch_workernode_list(integer()) -> ok.
watch_workernode_list(TimeInterval) ->
    EtcdKey = get_node_prefix(worker),
    erlang:send(?SERVER, {watch_workernode_list, EtcdKey, TimeInterval}),
    ok.

-spec register_workernode(atom(), integer(), integer()) -> ok.
register_workernode(Type, TTL, TimeInterval) ->
    register_workernode(Type, TTL, TimeInterval, node()).

-spec register_workernode(atom(), integer(), integer(), node()) -> ok.
register_workernode(Type, TTL, TimeInterval, Node) ->
    erlang:send(?SERVER, {register_workernode, Type, TTL, TimeInterval, Node}),
    ok.

-spec get_workernode(atom()) -> {error, term()} | {ok, node()}.
get_workernode(Type) ->
    EtcdKey = filename:join([get_node_prefix(worker), erlang:atom_to_list(Type)]),
    case catch ets:lookup(?SERVER, EtcdKey) of
        {'EXIT', _} ->
            {error, etcdc_cache_error};
        [] ->
            {error, empty};
        [{_, List}] ->
            {A1, A2, A3} = os:timestamp(),
            random:seed(A1, A2, A3),
            {ok, lists:nth(random:uniform(length(List)), List)}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?SERVER, [named_table, {read_concurrency, true}]),
    {ok, #state{}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_info({register_workernode, Type, TTL, TimeInterval, Node}, State) ->
    EtcdKey = generate_node_key(worker, Type, Node),
    write_node(EtcdKey, TTL),
    erlang:send_after(TimeInterval, self(),
                      {register_workernode, Type, TTL, TimeInterval, Node}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({watch_workernode_list, EtcdKey, TimeInterval}, State) ->
    case catch get_node_list(EtcdKey) of
        {'EXIT', _EXIT} ->
            ignore;
        Value ->
            catch update_node_list_into_ets(Value)
    end,
    erlang:send_after(TimeInterval, self(),
                      {watch_workernode_list, EtcdKey, TimeInterval}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_node_key(NodeType, Type, Node) ->
    KeyPrefix = get_node_prefix(NodeType),
    filename:join([KeyPrefix, erlang:atom_to_list(Type),
                   http_uri:encode(erlang:atom_to_list(Node))]).

write_node(EtcdKey, TTL) ->
    etcdc:set(EtcdKey, "up", [{ttl, TTL}]).

get_node_list(EtcdKey) ->
    etcdc:get(EtcdKey, [recursive]).

update_node_list_into_ets(EtcdRes) ->
    Nodes = maps:get(<<"nodes">>, maps:get(<<"node">>, EtcdRes)),
    [begin
        Key   = erlang:binary_to_list(maps:get(<<"key">>, Node)),
        Value = [begin
                    OriginKey = erlang:binary_to_list(maps:get(<<"key">>, X)),
                    OriginWorkerNode = filename:basename(OriginKey),
                    erlang:list_to_atom(http_uri:decode(OriginWorkerNode))
                 end || X <- maps:get(<<"nodes">>, Node)],
        ets:insert(?SERVER, {Key, Value})
     end || Node <- Nodes],
    ok.

get_node_prefix(worker) ->
    application:get_env(etcdc, ejabberd_workernode_prefix, "");
get_node_prefix(conn) ->
    application:get_env(etcdc, ejabberd_connnode_prefix, "");
get_node_prefix(muc) ->
    application:get_env(etcdc, ejabberd_mucnode_prefix, "");
get_node_prefix(_) ->
    "".
