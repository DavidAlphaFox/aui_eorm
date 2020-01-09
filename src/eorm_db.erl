-module(eorm_db).

-compile([export_all]).

%% @doc
%% add delete on 2019-3-26 by qibinlin
%% @end

delete(ReplyType, Query) when not is_map(ReplyType) ->
    Entity = eorm:get_entity(ReplyType),
    delete(Entity, Query);
delete(Entity, Query) ->
    delete(undefined, Entity, Query).

delete(Connection, ReplyType, Query) when not is_map(ReplyType) ->
    Entity = eorm:get_entity(ReplyType),
    delete(Connection, Entity, Query);

delete(Conn, Entity, InQuery) ->
    State = eorm_builder_delete:build(Entity, InQuery),
    #{
        'query' := Query,
        expr := #{
            bindings := Bindings,
            sql := SqlQuery
        }
    } = State,
    Connection = case Conn of
                     undefined -> eorm:get_connection(Entity, {delete, Query});
                     _ -> Conn
                 end,
    case Query of
        #{as_sql := true} -> {ok, SqlQuery};
        _ ->
            erlz:error_do([
                erlz:partial(fun exec_query/3, [Connection, SqlQuery, Bindings])
                ,fun({Count, []}) ->
                    {ok,Count}
                 end
            ])
    end.

%%==================================================

select(ReplyType, Query) when not is_map(ReplyType) ->
    Entity = eorm:get_entity(ReplyType),
    select(Entity, Query);
select(Entity, Query) ->
    select(undefined, Entity, Query).

select(Connection, ReplyType, Query) when not is_map(ReplyType) ->
    Entity = eorm:get_entity(ReplyType),
    select(Connection, Entity, Query);

select(Conn, Entity, InQuery) ->
    State = eorm_builder_select:build(Entity, InQuery),
    #{
        'query' := Query,
        expr := #{
            bindings := Bindings,
            sql := SqlQuery,
            fields := Fields
        }
    } = State,
    Connection = case Conn of
        undefined -> eorm:get_connection(Entity, {select, Query});
        _ -> Conn
    end,
    case Query of
        #{as_sql := true} -> {ok, SqlQuery};
        _ ->
            erlz:error_do([
                erlz:partial(fun exec_query/3, [Connection, SqlQuery, Bindings])
                ,fun(Rows) ->
                    Objs = lists:map(
                        fun(Row) -> row_to_object(Fields, Row) end, Rows),
                    {ok, Objs}
                end
                % TODO: add supporting as_sql
                ,erlz:partial(fun select_relates/3, [Connection, State])
            ])
    end.

row_to_object(Fields, InRow) ->
    {[Obj|Objs], _} = lists:foldl(
        fun({{Type,_Table,DataSourceName}, TFields}, {Acc, Row}) ->
            Head = lists:sublist(Row, 1, length(TFields)),
            Tail = lists:sublist(Row, length(TFields)+1, length(Row)),
            Attrs = maps:from_list(Head),

            Obj = eorm:transform_from(db, eorm_object:new({Type,DataSourceName}, Attrs)),
            {Acc ++ [Obj], Tail}
        end,
        {[], InRow},
        Fields),
    eorm_object:append_linked(Objs, Obj).

select_relates(_Connection, _, []) ->
    {ok, []};
select_relates(_Connection, #{expr:=#{extra_query:=[]}}, Objs) ->
    {ok, Objs};
select_relates(Connection, #{expr:=#{extra_query:=ExtraQuery,extra_info :=ExtraInfo}} = _State, Objs) ->
    IdField =
        case maps:get(id_field, ExtraInfo,undefined) of
            undefined -> <<"id">> ;
            Field -> eorm_utils:to_lower_bin(Field)

        end,
    Ids =
        lists:map(fun(Obj) ->
            eorm_object:get_attr(IdField,Obj)
        end, Objs),

    %%Ids = lists:map(fun eorm_object:id/1, Objs),

    erlz:error_foldlM(
        fun({Type, RelationKey, Query}, InObjs) ->
            Where = maps:get(where, Query, #{}),
            UpdQuery = Query#{where => Where#{{RelationKey, in} => Ids}},
            erlz:error_do([
                fun() -> select(Connection, Type, UpdQuery) end,
                erlz:partial(fun append_relates/3, [InObjs, {IdField,RelationKey}, '_'])
            ])
        end,
        Objs,
        ExtraQuery).

append_relates(InObjs,  {IdField,RelatesKey}, RelatesObjs) ->
    % by relations
    OutObjs = lists:foldl(
        fun(RelatedObj, Objs) ->
            % by objs for adding
            lists:map(
                fun(Obj) ->
                    case
                        eorm_object:get_attr( IdField ,Obj) ==
                        eorm_object:get_attr(RelatesKey, RelatedObj)
                    of
                        true -> eorm_object:append_linked(RelatedObj, Obj);
                        false -> Obj
                    end
                end,
                Objs)
        end,
        InObjs,
        lists:flatten(RelatesObjs)
    ),
    {ok, OutObjs}.


insert(Obj) ->
    insert(Obj, #{}).

%% insert without object
% eorm_query:insert(post,
%     #{
%         values => #{state => <<"">>},
%         returning => [id]
% }),
insert(Obj, Query) ->
    insert(undefined, Obj, Query).

insert(Conn, Obj, InQuery) ->
    %% 获得元信息
    Entity = eorm:get_entity(eorm_object:type(Obj)),
    DbObj = eorm:transform_to(db, Obj),
    State = eorm_builder_insert:build(Entity, DbObj, InQuery),
    #{
      'query':= Query,
      expr := #{
                sql := SQL,
                bindings := Bindings
               }
     } = State,
    Connection = case Conn of
        undefined -> eorm:get_connection(Entity, {insert, Query});
        _ -> Conn
    end,
    case Query of
        #{as_sql := true} -> {ok, SQL};
        _ ->
            erlz:error_do([
                           erlz:partial(fun exec_query/3, [Connection, SQL, Bindings])
                          ,fun({_Count, [Row]}) ->
                                   UpdObj = eorm_object:merge_attrs(
                                              maps:from_list(Row),
                                              Obj),
                                   {ok, UpdObj}
                           end
                          ])
    end.

update(Obj) ->
    update(undefined, Obj, #{}).

update(Obj, Query) ->
    update(undefined, Obj, Query).

update(Conn, Obj, InQuery) ->
    Entity = eorm:get_entity(eorm_object:type(Obj)),
    DbObj = eorm:transform_to(db, Obj),
    State = eorm_builder_update:build(Entity, DbObj, InQuery),
    #{
        'query':= Query,
        expr := #{
            sql := SQL,
            bindings := Bindings
        }
    } = State,
    Connection = case Conn of
        undefined -> eorm:get_connection(Entity, {update, Query});
        _ -> Conn
    end,
    case Query of
        #{as_sql := true} -> {ok, SQL};
        _ ->
            erlz:error_do([
                erlz:partial(fun exec_query/3, [Connection, SQL, Bindings])
                ,fun({_Count, [Row]}) ->
                    UpdObj = eorm_object:merge_attrs(
                        maps:from_list(Row),
                        Obj),
                    {ok, UpdObj}
                end
            ])
    end.

exec_query({Adapter, Connection}, Query, Bindings) ->
    Adapter:exec_query(Connection, Query, Bindings).
