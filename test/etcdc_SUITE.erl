%% common_test suite for etcdc

-module(etcdc_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(t_ct_utils, [ set_opt/3
                    , unique_user/1
                    , set_opt/2
                    , make_full_jib/3
                    , sender_send_msg_done/0
                    , receiver_receive_msg_done/0
                    , generate_one_user_login/1
                    ]).

%%--------------------------------------------------------------------
suite() -> [{timetrap, {seconds, 10}}].

%%--------------------------------------------------------------------
groups() ->
    [ {etcdc,
       [ test_whole_flow_workernode
       , test_etcd_watch
       ]}
    ].
%%--------------------------------------------------------------------
all() ->
    [{group, etcdc}].

%%--------------------------------------------------------------------
init_per_suite(Config) ->
    {ok, _} = etcdc:start(),
    ok = prepare_env(),
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
    application:set_env(etcdc, ejabberd_mucnode_prefix, "/imstest/ejabberd/mucnode"),
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

    ok = etcd_client:register_workernode(all, 5, 3000, 'test1@a'),
    ok = etcd_client:register_workernode(all, 5, 3000, 'test2'),
    ok = etcd_client:register_workernode(muc, 5, 3000, 'test3'),
    ok = etcd_client:register_workernode(muc, 5, 3000, 'test4'),

    ok = etcd_client:watch_workernode_list(1000),
    ok = wait_for_ets_table_ready(4),

    {ok, WorkerNode1} = etcd_client:get_workernode(all),
    true = lists:member(WorkerNode1, ['test1@a', 'test2']),
    {ok, WorkerNode2} = etcd_client:get_workernode(muc),
    true = lists:member(WorkerNode2, ['test3', 'test4']),
    {error, empty} = etcd_client:get_workernode(faketype),
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
