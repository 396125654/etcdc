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

-export([ get_machineid/0
        , get_machineid/1
        , watch_workernode_list/1
        , register_workernode/3
        , register_workernode/4
        , get_workernode/1
        , watch_storenode_list/1
        , register_storenode/3
        , register_storenode/4
        , get_storenode/1
        , unregister_workernode/1
        , unregister_workernode/2
        , unregister_storenode/1
        , unregister_storenode/2

        , config_watch_key/1
        , config_unwatch_key/1
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

get_machineid() ->
    get_machineid(node()).

get_machineid(Node) ->
    case get_machineid_self(Node) of
        {error, not_found} ->
            get_machineid_new(Node);
        {ok, MachineToID} ->
            MachineToID
    end.

-spec watch_workernode_list(integer()) -> ok.
watch_workernode_list(TimeInterval) ->
    watch_node_list(worker, TimeInterval).

-spec register_workernode(atom(), integer(), integer()) -> ok.
register_workernode(GroupType, TTL, TimeInterval) ->
    register_node(worker, GroupType, TTL, TimeInterval, node()).

-spec register_workernode(atom(), integer(), integer(), node()) -> ok.
register_workernode(GroupType, TTL, TimeInterval, Node) ->
    register_node(worker, GroupType, TTL, TimeInterval, Node).

-spec get_workernode(atom()) -> {error, term()} | {ok, node()}.
get_workernode(GroupType) ->
    get_node(worker, GroupType).

-spec watch_storenode_list(integer()) -> ok.
watch_storenode_list(TimeInterval) ->
    watch_node_list(store, TimeInterval).

-spec register_storenode(atom(), integer(), integer()) -> ok.
register_storenode(GroupType, TTL, TimeInterval) ->
    register_node(store, GroupType, TTL, TimeInterval, node()).

-spec register_storenode(atom(), integer(), integer(), node()) -> ok.
register_storenode(GroupType, TTL, TimeInterval, Node) ->
    register_node(store, GroupType, TTL, TimeInterval, Node).

-spec get_storenode(atom()) -> {error, term()} | {ok, node()}.
get_storenode(GroupType) ->
    get_node(store, GroupType).

unregister_workernode(GroupType) ->
    unregister_workernode(GroupType, node()).

unregister_workernode(GroupType, Node) ->
    unregister_node(worker, GroupType, Node).

unregister_storenode(GroupType) ->
    unregister_storenode(GroupType, node()).

unregister_storenode(GroupType, Node) ->
    unregister_node(store, GroupType, Node).

config_watch_key(EtcdKeyPrefix) ->
    gen_server:call(?SERVER, {config_watch_key, EtcdKeyPrefix}).

config_unwatch_key(EtcdKeyPrefix) ->
    gen_server:call(?SERVER, {config_unwatch_key, EtcdKeyPrefix}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?SERVER, [named_table, {read_concurrency, true}]),
    {ok, #state{}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------

handle_call({config_watch_key, EtcdKeyPrefix}, _From, State) ->
    case ets:lookup(?SERVER, {config_watch_key, EtcdKeyPrefix}) of
        [] ->
            etcdc:watch(EtcdKeyPrefix, [continous, recursive]),
            ok;
        [{_, _}] ->
            ok
    end,
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({config_unwatch_key, EtcdKeyPrefix}, _From, State) ->
    case ets:lookup(?SERVER, {config_watch_key, EtcdKeyPrefix}) of
        [{_, Pid}] ->
            ets:delete(?SERVER, {config_watch_key, EtcdKeyPrefix}),
            ok = etcdc:cancel_watch(Pid);
        [] ->
            ok
    end,
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({unregister_node, NodeType, GroupType, Node}, _From, State) ->
    EtcdKey = generate_node_key(NodeType, GroupType, Node),
    delete_node(EtcdKey),
    rewrite_ets_del_node(EtcdKey, Node),
    write_unregister_tag(Node),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_info({register_node, NodeType, GroupType, TTL, TimeInterval, Node},
            State) ->
    case ets:lookup(?SERVER, {'__UNREGISTER_TAG__', Node}) of
        [] ->
            EtcdKey = generate_node_key(NodeType, GroupType, Node),
            write_node(EtcdKey, TTL),
            erlang:send_after(TimeInterval, self(),
                              {register_node, NodeType, GroupType, TTL,
                               TimeInterval, Node});
        _ ->
            ok
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({watch_node_list, EtcdKey, TimeInterval}, State) ->
    case catch get_node_list(EtcdKey) of
        {'EXIT', _EXIT} ->
            ignore;
        Value ->
            catch update_node_list_into_ets(Value)
    end,
    erlang:send_after(TimeInterval, self(),
                      {watch_node_list, EtcdKey, TimeInterval}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({watch, _, EtcdConfigAltera}, State) ->
    etcd_config:set_env_and_backup_file_basedon_etcd(
        {watch, EtcdConfigAltera}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({watch_started, Pid, EtcdKeyPrefix}, State) ->
    ets:insert(?SERVER, {{config_watch_key, EtcdKeyPrefix}, Pid}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({watch_error, _, Key, Error}, State) ->
    io:format(" !! watch etcd error, key ~p, error ~p~n", [Key, Error]),
    ets:delete(?SERVER, {config_watch_key, Key}),
    erlang:send_after(3000, self(), {retry_watch_etcd, Key}),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({retry_watch_etcd, Key}, State) ->
    etcdc:watch(Key, [continous, recursive]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
    io:format("----->>>>>>>>>>>> ~p~n", [_Info]),
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

get_machineid_self(Node) ->
    MachineToIDKey =
        get_machine_to_id_key(http_uri:encode(atom_to_list(Node))),
    case etcdc:get(MachineToIDKey) of
        {error, _} ->
            {error, not_found};
        #{<<"node">> := #{<<"value">> := MachineToID0}} ->
            {ok, erlang:binary_to_integer(MachineToID0)}
    end.

write_machineid(MachineToID, Node) ->
    MachineToIDKey =
        get_machine_to_id_key(http_uri:encode(atom_to_list(Node))),
    IDToMachineKey = get_id_to_machine_key(MachineToID),
    etcdc:set(MachineToIDKey, MachineToID),
    etcdc:set(IDToMachineKey, etcd_data:encode_value(Node)),
    erlang:binary_to_integer(MachineToID).

get_machineid_new(Node) ->
    {ok, GlobalMachineIDKey} =
        application:get_env(etcdc, global_machine_id_key),
    case etcdc:get(GlobalMachineIDKey) of
        {error, not_found} ->
            {ok, GlobalMachineIDStart} =
                application:get_env(etcdc, global_machine_id_start),
            case etcdc:set(GlobalMachineIDKey, GlobalMachineIDStart,
                           [{prevExist, false}]) of
                {error, _} ->
                    get_machineid_new(Node);
                #{<<"action">> := <<"create">>,
                  <<"node">> := #{<<"value">> := MachineID}} ->
                    write_machineid(MachineID, Node)
            end;
        #{<<"node">> := #{<<"value">> := OriginMachineID0}} ->
            OriginMachineID = erlang:binary_to_integer(OriginMachineID0),
            NewMachineID = OriginMachineID + 1,
            case etcdc:set(GlobalMachineIDKey, NewMachineID,
                           [{prevValue, OriginMachineID}]) of
                {error, _} ->
                    get_machineid_new(Node);
                #{<<"action">> := <<"compareAndSwap">>,
                  <<"node">> := #{<<"value">> := NewOriginMachineID}} ->
                    write_machineid(NewOriginMachineID, Node)
            end
    end.

-spec watch_node_list(atom(), integer()) -> ok.
watch_node_list(NodeType, TimeInterval) ->
    EtcdKey = get_node_prefix(NodeType),
    erlang:send(?SERVER, {watch_node_list, EtcdKey, TimeInterval}),
    ok.

-spec register_node(atom(), atom(), integer(), integer(), node()) -> ok.
register_node(NodeType, GroupType, TTL, TimeInterval, Node) ->
    erlang:send(?SERVER, {register_node, NodeType, GroupType,
                          TTL, TimeInterval, Node}),
    ok.

-spec get_node(atom(), atom()) -> {error, term()} | {ok, node()}.
get_node(NodeType, GroupType) ->
    EtcdKey = filename:join([get_node_prefix(NodeType),
                             erlang:atom_to_list(GroupType)]),
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

unregister_node(NodeType, GroupType, Node) ->
    gen_server:call(?SERVER, {unregister_node, NodeType, GroupType, Node}).

generate_node_key(NodeType, GroupType, Node) ->
    KeyPrefix = get_node_prefix(NodeType),
    filename:join([KeyPrefix, erlang:atom_to_list(GroupType),
                   http_uri:encode(erlang:atom_to_list(Node))]).

delete_node(EtcdKey) ->
    etcdc:del(EtcdKey).

rewrite_ets_del_node(EtcdKey0, Node) ->
    EtcdKey = filename:dirname(EtcdKey0),
    case catch ets:lookup(?SERVER, EtcdKey) of
        [{EtcdKey, NodeList}] ->
            case lists:delete(Node, NodeList) of
                [] ->
                    ets:delete(?SERVER, EtcdKey);
                NewNodeList ->
                    ets:insert(?SERVER, {EtcdKey, NewNodeList})
            end,
            ok;
        _ ->
            ok
    end.

write_unregister_tag(Node) ->
    ets:insert(?SERVER, {{'__UNREGISTER_TAG__', Node}, fake}),
    ok.

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

get_machine_to_id_key(Machine) ->
    filename:join([get_machine_to_id_prefix(), Machine]).
get_id_to_machine_key(ID) ->
    filename:join([get_id_to_machine_prefix(), ID]).

get_machine_to_id_prefix() ->
    application:get_env(etcdc, ejabberd_machine_id_prefix, "").

get_id_to_machine_prefix() ->
    application:get_env(etcdc, ejabberd_id_machine_prefix, "").

get_node_prefix(worker) ->
    application:get_env(etcdc, ejabberd_workernode_prefix, "");
get_node_prefix(conn) ->
    application:get_env(etcdc, ejabberd_connnode_prefix, "");
get_node_prefix(store) ->
    application:get_env(etcdc, ejabberd_storenode_prefix, "");
get_node_prefix(_) ->
    "".
