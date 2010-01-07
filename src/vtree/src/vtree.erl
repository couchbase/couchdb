-module(vtree).

-export([lookup/2, within/2, intersect/2, disjoint/2, insert/2]).

% The bounding box coordinate order follows the GeoJSON specification (http://geojson.org/): {x-low, y-low, x-high, y-high}

% Design question: Should not fully filled nodes have only as many members as nodes, or be filled up with nils to their maximum number of nodes? - Current implementation is the first one (dynamic number of members).

lookup(Bbox, []) ->
    [];

lookup(Bbox, Tree) ->
    Entries = lists:foldl(fun(Entry, Acc) ->
        case Entry of
            % Inner node
            {Mbr, ChildNodes} when is_list(ChildNodes) ->
                lookup(Bbox, ChildNodes);
            % Leaf
            {Mbr, _} ->
                Disjoint = disjoint(Mbr, Bbox),
                if 
                    not Disjoint ->
                        Acc ++ [Entry];
                    true ->
                        Acc
                end;
            _ ->
                io:format("Tree/node is invalid"),
                error
        end
    end,
    [],
    Tree),
    Entries.

% Tests if Inner is within Outer box
within(Inner, Outer) ->
    {IW, IS, IE, IN} = Inner,
    {OW, OS, OE, ON} = Outer,
    (IW >= OW) and (IS >= OS) and (IE =< OE) and (IN =< ON).

% Returns true if one Mbr intersects with another Mbr
intersect(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    % N or S of Mbr1 is potentially intersected with a vertical edge
    % from Mbr2
    ((((N2 >= N1) and (S2 =< N1)) or ((N2 >= S1) and (S2 =< S1))) and
    % N or S of Mbr1 *is* intersected if a vertical line of Mbr2 isn't
    % next to Mbr1
    (((W2 >= W1) and (W2 =< E1)) or ((E2 >= W1) and (E2 =< E1))))
    or
    % W or E of Mbr1 is potentially intersected with a horizontal edge
    % from Mbr2
    ((((E2 >= E1) and (W2 =< E1)) or ((E2 >= W1) and (W2 =< W1))) and
    % W or E of Mbr1 *is* intersected if a horizontal line of Mbr2 isn't
    % above or below Mbr1
    (((S2 >= S1) and (S2 =< N1)) or ((N2 >= S1) and (N2 =< N1)))).

% Returns true if two MBRs are spatially disjoint
disjoint(Mbr1, Mbr2) ->
    not (within(Mbr1, Mbr2) or within(Mbr2, Mbr1) or intersect(Mbr1, Mbr2)).

insert(Mbr, []) ->
    [Mbr].
