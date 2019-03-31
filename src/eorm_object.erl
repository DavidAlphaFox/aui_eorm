-module(eorm_object).

-compile([export_all]).

new(Type, Attrs) when is_atom(Type) ->
    new(atom_to_binary(Type, utf8), Attrs);
new(Type, Attrs) ->
    #{
        type => Type,
        attributes => Attrs,
        linked => #{}
    }.

id(#{attributes := Attrs} = _Obj) ->
    maps:get(<<"id">>, Attrs).

type(Obj) ->
    maps:get(type, Obj).

linked(Obj) ->
    maps:get(linked, Obj).

linked(LinkedType, Obj) ->
    maps:get(LinkedType, linked(Obj)).

set_linked(Linked, Obj) ->
    Obj#{linked => Linked}.

% attrs

attrs(Obj) ->
    maps:get(attributes, Obj).

set_attrs(NewAttrs, Obj) ->
    Obj#{attributes => NewAttrs}.

attr(Key, Obj) ->
    get_attr(Key, Obj).

attr(Key, Obj, DefValue) ->
    get_attr(Key, Obj, DefValue).

get_attr(Key, #{attributes:=Attrs} = _Obj) ->
    '-get_attr'(Key,Attrs).
    %% maps:get(eorm_utils:to_binary(Key), Attrs).

get_attr(Key, #{attributes:=Attrs}, DefValue) ->
    case '-get_attr'(Key,Attrs) of
        undefined -> DefValue;
        Val -> Val
    end.
    %% maps:get(eorm_utils:to_binary(Key), Attrs, DefValue).

'-get_attr'(Key,Attrs) ->
    KeyBin = eorm_utils:to_binary(Key),
    case maps:get(KeyBin, Attrs,undefined) of
        undefined -> maps:get(eorm_utils:to_lower_bin(Key), Attrs,undefined);
        Val -> Val
    end.

set_attr(Key, Value, #{attributes := Attr} = Obj) ->
    Obj#{attributes => Attr#{Key => Value}}.

merge_attrs(NewAttrs, Obj) ->
    Attrs = attrs(Obj),
    Obj#{attributes => maps:merge(Attrs, NewAttrs)}.

attrs_with(Ks, Obj) ->
    #{attributes:=Attrs} = Obj,
    maps:with(Ks, Attrs).

unzip_attrs(Obj) ->
    #{attributes:=Attrs} = Obj,
    lists:unzip(maps:to_list(Attrs)).

unzip_attrs_with(Ks, Obj) ->
    NewAttrs = attrs_with(Ks, Obj),
    lists:unzip(maps:to_list(NewAttrs)).


append_linked(LinkObjs, ToObj_in) when is_list(LinkObjs) ->
    {ToObj,MasterTrackingId} =
        case get_attr(<<"MasterTrackingId">>,ToObj_in,nil) of
            nil ->
                TracingId = eorm_kv:masterTrackingId(),
                {set_attr(<<"MasterTrackingId">>,TracingId,ToObj_in),TracingId};
            TracingId -> {ToObj_in,TracingId}

        end,

    UpdLinked= lists:foldl(
        fun(LinkObj_orig, Acc) ->
            LinkObj =  set_attr(<<"MasterTrackingId">>,MasterTrackingId,LinkObj_orig),
            LinkType = type(LinkObj),
            Linked = maps:get(LinkType, Acc, []),
            Acc#{LinkType => Linked ++ [LinkObj]}
        end,
        linked(ToObj),
        LinkObjs),
    ToObj#{linked => UpdLinked};
append_linked(LinkObj, ToObj) when is_map(LinkObj) ->
    AllLinked = linked(ToObj),
    LinkType = type(LinkObj),
    Linked = maps:get(LinkType, AllLinked, []),
    ToObj#{linked => AllLinked#{LinkType => Linked ++ [LinkObj]}}.

get_linked(Type, Obj) ->
    AllLinked = linked(Obj),
    Linked = maps:get(Type, AllLinked, []),
    Linked.
