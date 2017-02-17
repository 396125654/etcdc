%% common_test suite for etcdc

-module(etcd_config_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%%--------------------------------------------------------------------
suite() -> [{timetrap, {seconds, 50}}].

%%--------------------------------------------------------------------
groups() ->
    [ {etcdc, [sequence],
       [ test_read_configure_from_etcd
       , test_config_local_migrate_etcd
       , test_enable_etcd_config
       , test_disable_etcd_config
       ]}
    ].
%%--------------------------------------------------------------------
all() ->
    [{group, etcdc}].

%%--------------------------------------------------------------------
init_per_suite(Config) ->
    {ok, _} = etcdc:start(),
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
    ok = prepare_env(),
    etcdc:del("/imstest", [recursive]),
    {ok, _} = etcd_client:start_link(),
    ok = etcd_config:just_for_test(),
    Config.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok = etcd_client:stop(),
    true = ets:delete(etcd_config),
    ok.

%%--------------------------------------------------------------------

prepare_env() ->
    application:set_env(etcdc, etcd_server_list,
                        "127.0.0.1:59179,127.0.0.1:59279,127.0.0.1:59379,127.0.0.1:59479"),
    application:set_env(etcdc, etcd_config_enable_applist, [app1, app2, app3]),
    application:set_env(etcdc, etcd_config_backup_file_dir, "./ss_backup_file_dir"),
    application:set_env(etcdc, etcd_config_prefix, "/imstest/vip1/ejabberd/workerconfig/all"),
    ok.

test_read_configure_from_etcd(_Config) ->
    %% disable
    ok = etcd_config:read_configure_from_etcd(),
    %% enable, configure options empty
    ok = application:set_env(etcdc, enable_etcd_config, true),
    ok = etcd_config:read_configure_from_etcd(),
    %% set
    KeyPrefix = application:get_env(etcdc, etcd_config_prefix, ""),
    etcdc:set(KeyPrefix ++ "/app1/p1", "v1"),
    etcdc:set(KeyPrefix ++ "/app2/p2", "v2"),
    V3 = [{a, [a, <<"a">>, 3]}],
    EncodeV3 = lists:flatten(io_lib:format("~p", [V3])),
    etcdc:set(KeyPrefix ++ "/app3/p3", EncodeV3),
    timer:sleep(1000),
    v1 = etcd_config:get_env(app1, p1, ''),
    v2 = etcd_config:get_env(app2, p2, ''),
    V3 = etcd_config:get_env(app3, p3, ''),
    %% unwatch
    ok = etcd_client:config_unwatch_key(KeyPrefix),
    etcdc:set(KeyPrefix ++ "/app1/newp", "newv"),
    '' = etcd_config:get_env(app1, newp, ''),
    %% read again
    ok = etcd_config:read_configure_from_etcd(),
    {ok, FileContentBin} =
        file:read_file(application:get_env(etcdc, etcd_config_backup_file_dir, "") ++ "/app1"),
    newv = proplists:get_value(newp, etcd_config:parse_etcd_string(binary_to_list(FileContentBin))),
    %% unwatch
    ok = etcd_client:config_unwatch_key(KeyPrefix),
    application:set_env(etcdc, etcd_server_list, "127.0.0.1:59479"),
    ok = etcd_config:read_configure_from_etcd(),
    v1 = etcd_config:get_env(app1, p1, ''),
    v2 = etcd_config:get_env(app2, p2, ''),
    V3 = etcd_config:get_env(app3, p3, ''),
    newv = etcd_config:get_env(app1, newp, ''),
    ok.

test_config_local_migrate_etcd(_Config) ->
    ok = etcd_config:set_env(app1, pp1, vv1),
    ok = etcd_config:set_env(app2, pp2, vv2),

    ok = etcd_config:config_local_migrate_etcd(),

    timer:sleep(1000),
    KeyPrefix = application:get_env(etcdc, etcd_config_prefix, ""),
    #{<<"node">> := #{<<"value">> := <<"vv1">>}} =
        etcdc:get(KeyPrefix ++ "/app1/pp1"),
    #{<<"node">> := #{<<"value">> := <<"vv2">>}} =
        etcdc:get(KeyPrefix ++ "/app2/pp2"),
    ok.

test_enable_etcd_config(_Config) ->
    KeyPrefix = application:get_env(etcdc, etcd_config_prefix, ""),
    ok = etcd_config:enable_etcd_config(),
    etcdc:set(KeyPrefix ++ "/app1/p1", "v11"),
    etcdc:set(KeyPrefix ++ "/app2/p2", "v22"),
    timer:sleep(1000),
    v11 = etcd_config:get_env(app1, p1, ''),
    v22 = etcd_config:get_env(app2, p2, ''),
    ok = etcd_config:disable_etcd_config(),
    ok.

test_disable_etcd_config(_Config) ->
    KeyPrefix = application:get_env(etcdc, etcd_config_prefix, ""),
    application:set_env(etcdc, enable_etcd_config, true),
    ok = etcd_config:read_configure_from_etcd(),
    ok = etcd_config:disable_etcd_config(),
    etcdc:set(KeyPrefix ++ "/app111/p111", "v111"),
    timer:sleep(1000),
    '' = etcd_config:get_env(app111, p111, ''),
    ok.
