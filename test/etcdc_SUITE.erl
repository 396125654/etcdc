%% common_test suite for etcdc

-module(etcdc_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%%--------------------------------------------------------------------
suite() -> [{timetrap, {seconds, 30}}].

%%--------------------------------------------------------------------
groups() ->
    [ {etcdc, [sequence],
       [ test_whole_flow_workernode
       , test_whole_flow_storenode
       , test_whole_flow_node
       , test_etcd_watch
       , test_get_machineid_clean_history
       , test_get_machineid
       ]}
    ].
%%--------------------------------------------------------------------
all() ->
    [{group, etcdc}].

%%--------------------------------------------------------------------
init_per_suite(Config) ->
    {ok, _} = etcdc:start(),
    ok = prepare_env(),
    etcdc:del("/imstest", [recursive]),
    Config.

%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    etcdc:stop(),
    ok.

%%--------------------------------------------------------------------
init_per_group(_group, Config) ->
    Config.

%%--------------------------------------------------------------------
end_per_group(_group, _Config) ->
    ok.

%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------

prepare_env() ->
    application:set_env(etcdc, etcd_server_list,
                        [{"127.0.0.1", 49179}, {"127.0.0.1", 49279},
                         {"127.0.0.1", 49379}, {"127.0.0.1", 49479}]),
    application:set_env(etcdc, client_retry_times, 3),
    application:set_env(etcdc, ejabberd_workernode_prefix, "/imstest/ejabberd/workernode"),
    application:set_env(etcdc, ejabberd_connnode_prefix, "/imstest/ejabberd/connnode"),
    application:set_env(etcdc, ejabberd_storenode_prefix, "/imstest/ejabberd/storenode"),
    application:set_env(etcdc, global_machine_id_key, "/imstest/ejabberd/globalmachineid"),
    application:set_env(etcdc, global_machine_id_start, 1),
    application:set_env(etcdc, ejabberd_machine_id_prefix, "/imstest/ejabberd/machinetoid"),
    application:set_env(etcdc, ejabberd_id_machine_prefix, "/imstest/ejabberd/idtomachine"),
    ok.

wait_for_ets_table_ready(Len) ->
    Temp = lists:append([X || {_, X} <- ets:tab2list(etcd_client)]),
    case erlang:length(Temp) == Len of
        true ->
            ok;
        false ->
            timer:sleep(500),
            wait_for_ets_table_ready(Len)
    end.

wait_for_etcd_watch_event() ->
    receive
        {watch, _, Action} ->
            
            ok = match_key_val(Action, <<"/imstest/ejabberd/workerconfig/ejabberd/session_db">>,
                               "redis"),
            ok = match_key_val(Action, <<"/imstest/ejabberd/workerconfig/im_libs/redis">>,
                               ['index', <<"kafka">>])        
    after
        3000 ->
            erlang:exit("receive etcd watch event timeout")
    end.

match_key_val(Action, Key, Value) ->
    OriginKey = maps:get(<<"key">>, maps:get(<<"node">>, Action)),
    case OriginKey == Key of
        true ->
            Value = etcd_data:decode_value(maps:get(<<"value">>, maps:get(<<"node">>, Action)));
        false ->
            ok
    end,
    ok.

test_whole_flow_workernode(_Config) ->
    {ok, _} = etcd_client:start_link(),

    ok = etcd_client:register_workernode(all, 5, 300, 'test1@a'),
    ok = etcd_client:register_workernode(all, 5, 300),
    ok = etcd_client:register_workernode(muc, 5, 300, 'test3'),
    ok = etcd_client:register_workernode(muc, 5, 300, 'test4'),

    ok = etcd_client:watch_workernode_list(100),
    ok = wait_for_ets_table_ready(4),

    timer:sleep(2000),

    {ok, WorkerNode1} = etcd_client:get_workernode(all),
    true = lists:member(WorkerNode1, ['test1@a', node()]),
    {ok, WorkerNode2} = etcd_client:get_workernode(muc),
    true = lists:member(WorkerNode2, ['test3', 'test4']),
    {error, empty} = etcd_client:get_workernode(faketype),
    ok = etcd_client:stop(),
    {error, etcdc_cache_error} = etcd_client:get_workernode(faketype),
    ok.

test_whole_flow_storenode(_Config) ->
    {ok, _} = etcd_client:start_link(),

    ok = etcd_client:register_storenode(all, 5, 300, 'test1@a'),
    ok = etcd_client:register_storenode(all, 5, 300),
    ok = etcd_client:register_storenode(muc, 5, 300, 'test3'),
    ok = etcd_client:register_storenode(muc, 5, 300, 'test4'),

    ok = etcd_client:watch_storenode_list(100),
    ok = wait_for_ets_table_ready(4),

    timer:sleep(2000),

    {ok, StoreNode1} = etcd_client:get_storenode(all),
    true = lists:member(StoreNode1, ['test1@a', node()]),
    {ok, StoreNode2} = etcd_client:get_storenode(muc),
    true = lists:member(StoreNode2, ['test3', 'test4']),
    {error, empty} = etcd_client:get_storenode(faketype),
    ok = etcd_client:stop(),
    {error, etcdc_cache_error} = etcd_client:get_storenode(faketype),
    ok.

test_whole_flow_node(_Config) ->
    {ok, _} = etcd_client:start_link(),

    ok = etcd_client:register_workernode(all, 5, 300, 'test1@a'),
    ok = etcd_client:register_workernode(all, 5, 300),
    ok = etcd_client:register_workernode(muc, 5, 300, 'test3'),
    ok = etcd_client:register_workernode(muc, 5, 300, 'test4'),
    ok = etcd_client:watch_workernode_list(100),

    ok = etcd_client:register_storenode(all, 5, 300, 'test1@a'),
    ok = etcd_client:register_storenode(all, 5, 300),
    ok = etcd_client:register_storenode(muc, 5, 300, 'test3'),
    ok = etcd_client:register_storenode(muc, 5, 300, 'test4'),
    ok = etcd_client:watch_storenode_list(100),

    ok = wait_for_ets_table_ready(8),

    timer:sleep(2000),

    {ok, WorkerNode1} = etcd_client:get_workernode(all),
    true = lists:member(WorkerNode1, ['test1@a', node()]),
    {ok, WorkerNode2} = etcd_client:get_workernode(muc),
    true = lists:member(WorkerNode2, ['test3', 'test4']),
    {error, empty} = etcd_client:get_workernode(faketype),

    {ok, StoreNode1} = etcd_client:get_storenode(all),
    true = lists:member(StoreNode1, ['test1@a', node()]),
    {ok, StoreNode2} = etcd_client:get_storenode(muc),
    true = lists:member(StoreNode2, ['test3', 'test4']),
    {error, empty} = etcd_client:get_storenode(faketype),

    ok = etcd_client:stop(),
    {error, etcdc_cache_error} = etcd_client:get_workernode(faketype),
    ok.

test_etcd_watch(_Config) ->
    etcdc:watch("/imstest/ejabberd/workerconfig", [recursive, continous]),
    etcdc:set("/imstest/ejabberd/workerconfig/ejabberd/session_db",
              etcd_data:encode_value("redis")),
    etcdc:set("/imstest/ejabberd/workerconfig/im_libs/redis",
              etcd_data:encode_value(['index', <<"kafka">>])),

    wait_for_etcd_watch_event(),
    wait_for_etcd_watch_event(),

    ok.

test_get_machineid(_Config) ->
    test_get_machineid_do().

test_get_machineid_clean_history(_Config) ->
    etcdc:del("/imstest", [recursive]),
    test_get_machineid_do().

test_get_machineid_do() ->
    T = ets:new(t, [public]),
    RefList =
        [task:async(fun() -> Node = list_to_atom("node" ++ integer_to_list(X)),
                             ID = etcd_client:get_machineid(Node),
                             ets:insert(T, {Node, ID}),
                             ets:insert(T, {ID, Node}),
                             ok
                    end)
        || X <- lists:seq(1, 20)],
    [ok = task:await(X) || X <- RefList],
    SlefNode = node(),
    SlefID = etcd_client:get_machineid(),
    ets:insert(T, {SlefNode, SlefID}),
    ets:insert(T, {SlefID, SlefNode}),
    EtsList = ets:tab2list(T),
    [1 = erlang:length(proplists:get_all_values(ID, EtsList))
     || {ID, _Node} <- EtsList],
    [true = (ID == proplists:get_value(Node, EtsList))
     || {ID, Node} <- EtsList, erlang:is_integer(ID) == true],
    ok.
