-module(eorm).

-export([
    init/0
    ,destroy/0
    ,def_db/1
    ,cb_prepare_statement/2
    ,cb_prepare_obj_statement/3
    ,get_table/2
    ,get_connection/2
    ,def_entity/2
    ,get_entity/1
    ,transform_to/2
    ,transform_from/2

]).

init() ->
    application:ensure_all_started(pgapp),
    spawn(fun() ->
        ?MODULE = ets:new(?MODULE, [
            named_table,
            public,
            {read_concurrency, true}
        ]),
        receive X -> X,ok end
    end).

destroy() ->
    ets:delete(?MODULE).

def_db(_DbDeclaration) ->
    throw({ton_implemented}).

cb_prepare_statement(
        #{callbacks := #{prepare_statement:=Fn}} = _Entity, Query) ->
    Fn(Query);
cb_prepare_statement(_Entity, {_Kind, Query}) ->
    Query.

cb_prepare_obj_statement(
        #{callbacks := #{prepare_obj_statement:=Fn}} = _Entity, Obj, Query) ->
    Fn(Obj, Query);
cb_prepare_obj_statement(_Entity, _Obj, {_Kind, Query}) ->
    Query.
%% 构建关系
prepare_relationship(_Kind, _FromType, {ToType, Field}) ->
    {eorm_utils:to_binary(ToType), eorm_utils:to_binary(Field)};

prepare_relationship('has-many', FromType, ToType) ->
    {eorm_utils:to_binary(ToType), <<FromType/binary, "_id">>};

prepare_relationship('belongs-to', _FromType, ToType) ->
    BinToType = eorm_utils:to_binary(ToType),
    {BinToType, <<BinToType/binary, "_id">>};

prepare_relationship('has-one', FromType, ToType) ->
    BinToType = eorm_utils:to_binary(ToType),
    {BinToType, <<FromType/binary, "_id">>}.

prepare_relationships(#{type:=FromType} = Entity) ->
    %% 实体类型是默认的from
    lists:foldl(
        fun(Kind, Acc) ->
                %% 的到所有的关联表
                ToTypes = maps:get(Kind, Entity, []),
                lists:foldl(
                  fun(ToType, InAcc) ->
                          %% 遍历所有的表
                          %% 我们假定from是user，to是user
                          %% has-one 生成 {<<"email">>,<<"user_id">>}
                          %% belongs-to生成 {<<"email">>,<<"email_id">>}
                          %% has-many 生成 {<<"email">>,<<"user_id"}
                          {T, F} = prepare_relationship(Kind, FromType, ToType),
                          %% 保存成 #{<<"email">> => {'has-many',<<"user_id">>}}
                          InAcc#{T => {Kind, F}}
                  end,Acc,ToTypes)
        end, #{}, ['has-many', 'has-one', 'belongs-to']).

get_table(#{table:=GetFn} = _Entity, Query) when is_function(GetFn) ->
    GetFn(Query);
get_table(#{table:=Table} = _Entity, _Query) ->
    eorm_utils:to_binary(Table).


get_connection(#{
        db_connection:=GetFn,
        db_adapter:=Adapter} = _Entity, Query) when is_function(GetFn) ->
    {Adapter, GetFn(Query)};
get_connection(#{
        db_connection:=Connection,
        db_adapter:=Adapter} = _Entity, _Query) ->
    {Adapter, Connection};
get_connection(#{db_adapter:=Adapter} = _Entity, _Query) -> {Adapter, undefined}.
    %%throw({bad_entity, no_connection}).
%% 反射成table对象
reflect_table(#{fields := Fields} = Entity) ->
    Entity#{
        fields => lists:map(fun eorm_utils:to_binary/1, Fields)
    };
reflect_table(Entity) ->
    ReflectQuery = {reflect, #{}},
    %% 得到表名
    Table = get_table(Entity, ReflectQuery),
    %% 得到数据库链接
    Connection = get_connection(Entity, ReflectQuery),
    case
        %% 读取数据库中的表定义
        eorm_db:exec_query(Connection, <<
            "select array(select column_name::text from information_schema.columns ",
            "where table_schema=current_schema() and table_name = $1) as columns">>, [Table])
    of
        {ok, []} ->
            throw({error_reflect, {"Could not get columns from table ", Table}});
        {ok, [Row]} ->
            Entity#{
                fields => proplists:get_value(<<"columns">>, Row)};
        {error, Reason} ->
            throw({error_reflect, Reason})
    end.
%% 定义实体
def_entity(InType, InEntity) ->
    %% 将实体的类型转化binary
    Type = eorm_utils:to_binary(InType),
    Entity = InEntity#{type => Type},
    Adapter = maps:get(db_adapter, Entity, adapter_epgsql),
    % normalization
    UpdEntity = reflect_table(Entity#{
                                      db_adapter => Adapter,
                                      %% 生成关系表
                                      relationships => prepare_relationships(Entity),
                                      %% 修改主键类型
                                      pk => eorm_utils:to_binary(maps:get(pk, Entity, <<"id">>))
                                     }),
    %% 保存到全局的ets中
    true = ets:insert(?MODULE, {{type, Type}, UpdEntity}).

get_entity(Type) ->
    '-get_entity'( eorm_utils:to_binary(Type)).
%% 获得某个实体的元信息描述
%% @doc @private
'-get_entity'(Type) when is_binary(Type)->
    case ets:member(?MODULE, {type, Type}) of
        true ->
            [{{type, _Type}, Entity}] =  ets:lookup(?MODULE, {type, Type}),
            Entity;

        fasle -> entity_not_exists
    end.



transform_to(Format, Obj) ->
    transform('transform-to', Format, Obj).


transform_from(Format, Obj) ->
    transform('transform-from', Format, Obj).


transform(TransformKey, Format, #{type := Type} = Obj) ->
    Entity = '-get_entity'(Type),
    transform(TransformKey, Format, Obj, Entity).

transform(TransformKey, Format, Obj, Entity) ->
    #{linked := Linked} = Obj,
    case Entity of
        #{TransformKey := #{Format := Transforms}} ->
            UpdObj = transform_obj(Transforms, Obj),
            UpdLinked = maps:map(
                fun(_K, V) ->
                    lists:map(fun(LObj) -> transform(TransformKey, Format, LObj) end, V)
                end,
                Linked),
            UpdObj#{linked := UpdLinked};
        _ ->
            Obj
    end.

transform_obj(Transforms, Obj) when is_list(Transforms) ->
    lists:foldl(
        fun(Transform, InObj) -> transform_obj(Transform, InObj) end,
        Obj,
        Transforms);

transform_obj(Transform, Obj) when is_function(Transform)->
    Transform(Obj);

transform_obj(Transform, Obj) when is_map(Transform)->
    Attrs = eorm_object:attrs(Obj),
    UpdAttrs = maps:fold(
        fun(Key, Fn, AttrsIn) ->
            case maps:get(Key, AttrsIn, undefined) of
                undefined -> AttrsIn;
                V -> AttrsIn#{Key => Fn(V)}
            end
        end,
        Attrs, Transform),
    eorm_object:set_attrs(UpdAttrs, Obj).
