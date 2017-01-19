-module(etcd_data).

-export([decode_key/1, encode_key/1, decode_value/1, encode_value/1]).

encode_key([D | K])->
    lists:foldl(fun(K1, K2)->
                        K3 = encode_key(K1),
                        <<K2/binary, "/", K3/binary>>
                end, encode_key(D), K);
encode_key(K)when is_atom(K)->
    atom_to_binary(K, latin1);
encode_key(K)when is_binary(K)->
    <<"$", K/binary>>;
encode_key(K)when is_integer(K)->
    integer_to_binary(K).

decode_key(<<"$", K/binary>>)->
    K;
decode_key(K)when is_binary(K)->
    try
        jsx:decode(K)
    catch _:_ ->
              binary_to_atom(K, latin1)
    end.

decode_value(V)->
    V1 = jsx:decode(V),
    decode_value_1(V1).

encode_value(V)->
    V1 = encode_value_1(V),
    jsx:encode(V1).

decode_key_1(<<"$", K/binary>>)->
    K;
decode_key_1(K)when is_binary(K)->
    binary_to_atom(K, latin1);
decode_key_1(K)when is_integer(K)->
    K.

encode_key_1(K)when is_binary(K)->
    <<"$", K/binary>>;
encode_key_1(K)->
    K.

decode_value_1([{_,_}|_] = V)->
    lists:map(fun({K1, V1})->
                      {decode_key_1(K1), decode_value_1(V1)}
              end, V);
decode_value_1([_|_] = V)->
    lists:map(fun(V1)->
                      decode_value_1(V1)
              end, V);
decode_value_1(<<"$", V/binary>>)->
    V;
decode_value_1(V)when is_binary(V)->
    binary_to_atom(V, latin1);
decode_value_1(V)->
    V.

encode_value_1(V)when is_list(V)->
    lists:map(fun encode_value_1/1, V);
encode_value_1({K, V})->
    {encode_key_1(K), encode_value_1(V)};
encode_value_1(V)when is_binary(V)->
    <<"$", V/binary>>;
encode_value_1(V)->
    V.
