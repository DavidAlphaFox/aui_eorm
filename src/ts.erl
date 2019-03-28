-module(ts).

-export([
    init/0,t1/0
]).

-define(MAX_CHUNKS, 2).

t2() ->
    {ok, Conn1} = epgsql:connect(
        "127.0.0.1", "dbuser", "dbpassword", [
            {database, "testdb"},
            {timeout, 4000}
        ]),

    eorm:def_entity(purchLine, #{
        db_connection => Conn1,
        table => purchline,
        fields =>[purchId,inventDimId,itemId],
        pk => id,
        relations =>
            #{inventDim => [
                    %%{Kind,Field,RelatedField }
                    {normal ,inventDimId,inventDimId}
                ],

              purchTable => [
                    {normal,purchId,purchId}
                ],
              prodTable => [
                    {normal,inventRefId, prodId},
                    %% {Kind,Field ,Value}
                    {fieldFixed ,itemRefType,5}
                ],
              returnActionDefaults => [
                    {normal, returnActionId,returnActionId},
                    %% {Kind,RelatedField,Value}
                    {relatedFieldFixed , module, 1}
                ]
            }

    }),

    eorm:get_entity(purchLine).



t1() ->
    Entity = eorm:get_entity(user),

    %%#{db_connection := Conn } = Entity,

    {ok, Conn1} = epgsql:connect(
        "127.0.0.1", "dbuser", "dbpassword", [
            {database, "testdb"},
            {timeout, 4000}
        ]),
    Changed =
    #{
        db_connection =>  Conn1,
        table => users,
        pk => id,
        'has-many' => [post],
        'has-one' => [email]

    },
    eorm:def_entity(user,Changed),
    eorm:get_entity(user).

%%====================
compress(Data) ->
    compress(Data, zip).

decompress(Data) ->
    compress(Data, unzip).

compress(null, _Flag) ->
    null;
compress(Data, Flag) ->
    case Flag of
        zip ->
            zlib:zip(Data);
        unzip ->
            zlib:unzip(Data)
    end.




init() ->
    eorm:init(),
    % % TODO
    % eorm:def_db(test_db #{
    %     adapter => adapter_epgsql,
    %     host => "127.0.0.1",
    %     database => "testdb",
    %     user => "dbuser",
    %     password => "dbpassword",
    % }),

    % % TODO
    % eorm:def_db(test_db #{
    %     adapter => adapter_epgsql_pool,
    %     pool_name => test_pool
    % }),
    {ok, Conn1} = epgsql:connect(
        "127.0.0.1", "dbuser", "dbpassword", [
            {database, "testdb"},
            {timeout, 4000}
    ]),
    {ok, Conn2} = epgsql:connect(
        "127.0.0.1", "dbuser", "dbpassword", [
            {database, "testdb"},
            {timeout, 4000}
    ]),


    eorm:def_entity(user, #{
        db_connection => Conn1,
        table => users,
        pk => id,
        'has-many' => [post],
        'has-one' => [email]
    }),

    eorm:def_entity(email, #{
        db_connection => Conn1,
        table => user_emails,
        pk => id,
        'belongs-to' => [user]
    }),

    eorm:def_entity(post, #{
        db_connection => fun({_Kind, _Query}) ->
            lists:nth(crypto:rand_uniform(1,3), [Conn1, Conn2])
        end,
        table => fun({_Kind, Query}) ->
            % Kind :: reflect | select | update | insert | delete
            case Query of
                #{meta := #{chunk_id := ChunkId}} ->
                    list_to_binary(
                        lists:flatten(
                            io_lib:format(<<"posts_~2..0B">>, [ChunkId])));
                _ ->
                    <<"posts">>
            end
        end,
        pk => id,
        'belongs-to' => [
            user,
            % and the same but with specifying of field
            {user, user_id}
        ],
        'has-many' => [
            {post_action_log, action_post_id}
        ],
        'transform-from' => #{
            'db' => #{
                <<"data">> => fun decompress/1
            }
        },
        'transform-to' => #{
            'db' => #{
                <<"data">> => fun compress/1
            },
            json => [
                fun(Obj) ->
                    Obj#{meta => #{
                        <<"info">> => <<"example of transformation whole object">>}}
                end,
                #{
                    <<"created_at">> => fun(DateTime) ->
                        {{Year,Month,Day},{Hour,Min,Sec}} = DateTime,
                        iolist_to_binary(
                            io_lib:format(
                                "~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0fZ",
                                [Year, Month, Day, Hour, Min, Sec]))
                    end
                }
            ]
        },
        callbacks => #{
            prepare_obj_statement => fun(Obj, {_Kind, Query}) ->
                % Kind :: update | insert | delete
                UserId = eorm_object:get_attr(<<"user_id">>, Obj),
                ChunkId = ((UserId + 1) rem ?MAX_CHUNKS) + 1,
                Query#{meta => #{chunk_id => ChunkId}}
            end,
            prepare_statement => fun({_Kind, Query}) ->
                % Kind :: select
                case Query of
                    #{where := #{user_id := UserId}} ->
                        ChunkId = ((UserId + 1) rem ?MAX_CHUNKS) + 1,
                        Query#{meta => #{chunk_id => ChunkId}};
                    _ ->
                        Query
                        %throw({partitioning_condition, "Could not to detect chunk id"})
                end
            end
        }
    }),


    eorm:def_entity(post_action_log, #{
        db_connection => Conn1,
        table => post_actions_log,
        pk => id,
        'has-one' => [post]
    }),
    ok.
