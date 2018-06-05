-module(etcdc_lib).

-export([call/3, call/4, ensure_first_slash/1, parse_response/1]).

-export([url_encode/1, url_decode/1]).

-define(DEFAULT_TIMEOUT, timer:seconds(10)).

%% ----------------------------------------------------------------------------

call(Method, Path, Opts) ->
    call(Method, Path, Opts, <<>>).

call(Method, Path, Opts, Value) ->
    RetryTime = get_etcdc_call_retrytimes(),
    call_do(Method, Path, Opts, Value, RetryTime, []).

call_do(Method, Path, Opts, Value, RetryTime, Exclude) ->
    {Host, Port} = get_server_info(Exclude),
    Timeout = get_timeout(Opts),
    Url = url(Host, Port, Path, proplists:unfold(Opts)),
    case lhttpc:request(Url, Method, [], Value, Timeout) of
        {ok, {{Code, _}, _, Body}} when Code >= 200, Code =< 205 ->
            parse_response(Body);
        {ok, {{404, _}, _, _}} ->
            {error, not_found};
        {ok, {{_, _}, _, Body}} ->
            {error, parse_response(Body)};
        {error, Error} ->
            case retry_call(RetryTime) of
                true ->
                    call_do(Method, Path, Opts, Value,
                            RetryTime - 1, [{Host, Port} | Exclude]);
                false ->
                    {error, Error}
            end
    end.

ensure_first_slash([$/ | _] = Path) -> Path;
ensure_first_slash(Path) -> [$/ | Path].

parse_response(Body) ->
    jsx:decode(Body, [return_maps]).

%% ----------------------------------------------------------------------------

get_etcdc_call_retrytimes() ->
    application:get_env(etcdc, client_retry_times, 1).

retry_call(RetryTime) when RetryTime > 1 ->
    true;
retry_call(_) ->
    false.

get_server_info(Exclude) ->
    {ok, ServerString} = application:get_env(etcdc, etcd_server_list),
    ServerList0 =
        [begin
            [Host, Port] = string:tokens(X, ":"),
            {Host, erlang:list_to_integer(Port)}
         end || X <- string:tokens(ServerString, ",")],
    ServerList =
        case erlang:length(ServerList0) =< erlang:length(Exclude) of
            true ->
                ServerList0;
            false ->
                ServerList0 -- Exclude
        end,
    lists:nth(rand:uniform(length(ServerList)), ServerList).

get_timeout(Opts) ->
    case proplists:get_bool(wait, Opts) of
        true ->
            infinity;
        false ->
            proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT)
    end.

url(Host, Port, Path, Query) when is_integer(Port) ->
    Base  = ["http://", Host, ":", integer_to_list(Port)],
    NewPath = ensure_first_slash(Path),
    case parse_query(Query) of
        [] ->
            lists:flatten([Base, NewPath]);
        Qs ->
            lists:flatten([Base, NewPath, "?", Qs])
    end.

parse_query(Qs) ->
    string:join([to_str(K) ++ "=" ++ to_str(V) || {K, V} <- Qs], "&").

to_str(T) when is_atom(T) -> url_encode(atom_to_list(T));
to_str(T) when is_integer(T) -> url_encode(integer_to_list(T));
to_str(T) when is_float(T) -> url_encode(float_to_list(T));
to_str(T) when is_binary(T) -> url_encode(binary_to_list(T));
to_str(T) -> url_encode(T).

%% Url encode/decode ----------------------------------------------------------

url_encode(Value) ->
    encode_chars(Value, safe_chars(), []).

encode_chars([$\s | Value], Safe, Acc) ->
    encode_chars(Value, Safe, [$+ | Acc]);
encode_chars([C | Value], Safe, Acc) ->
    case lists:member(C, Safe) of
        true ->
            encode_chars(Value, Safe, [C | Acc]);
        false ->
           encode_chars(Value, Safe, [encode_char(C) | Acc])
    end;
encode_chars([], _, Acc) ->
    lists:reverse(Acc).

encode_char(Char) ->
    <<Hi:4, Lo:4>> = <<Char:8>>,
    [$%, to_hex(Hi), to_hex(Lo)].

url_decode(Value) ->
    decode_chars(Value, []).

decode_chars([$%, Hi, Lo|Rest], Res) ->
    <<Char:8>> = <<(from_hex(Hi)):4, (from_hex(Lo)):4>>,
    decode_chars(Rest, [Char|Res]);
decode_chars([C|Rest], Res) ->
    decode_chars(Rest, [C|Res]);
decode_chars([], Res) ->
    lists:reverse(Res).

to_hex(10) -> $a;
to_hex(11) -> $b;
to_hex(12) -> $c;
to_hex(13) -> $d;
to_hex(14) -> $e;
to_hex(15) -> $f;
to_hex(N) when N < 10 -> $0 + N.

from_hex($a) -> 10;
from_hex($b) -> 11;
from_hex($c) -> 12;
from_hex($d) -> 13;
from_hex($e) -> 14;
from_hex($f) -> 15;
from_hex(N) -> N-$0.

safe_chars() ->
    lists:append([lists:seq($a, $z),
                  lists:seq($A, $Z),
                  lists:seq($0, $9),
                  [$., $-, $~, $_]]).
