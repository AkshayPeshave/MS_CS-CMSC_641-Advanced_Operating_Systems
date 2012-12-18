-module(gossip_for_lists).
-compile([debug_info, export_all]).

partition(MyPartition, Neighbor, NeighborPartition, NodePartitionSize) -> 
	Fun = fun(Key, Value1, Value2) ->
			if 
				Value1>Value2 -> Value1;
				true -> Value2
			end
		end,

	PartitionUnion = dict:merge(Fun, MyPartition, NeighborPartition),
	
	PartitionUnionList = dict:to_list(PartitionUnion),
	MyNewPartition = dict:from_list(lists:sublist( lists:ukeysort(2,PartitionUnionList), NodePartitionSize)),

	MyNewPartition
.

choose_random_node(MyPartition) ->
	RandomNode = lists:nth(random:uniform(dict:size(MyPartition)),dict:fetch_keys(MyPartition)),
	MyUpdatedPartition = dict:store(RandomNode, dict:fetch(RandomNode,MyPartition)+1,MyPartition),
	[RandomNode, MyUpdatedPartition]
.

gossip(MyPartition, N, MyValues, Mode, RoundsRemaining, Delay) ->
	if Mode == readyToSend, RoundsRemaining > 0, Delay==0 ->
		RandomIndex = random:uniform(erlang:trunc(math:log(N)+1)),
		RandomNode = lists:nth(RandomIndex, MyPartition),
		io:format("~p gossipping with ~p~n",[self(), RandomNode]),
		io:format("Message : ~p~n~n",[{self(), send, avg, MyValues, MyPartition}]),
		RandomNode ! {self(), send, avg, MyValues, MyPartition},
		gossip(MyPartition, N, MyValues, awaitingReply, RoundsRemaining - 1, 0);
	
	true -> a
	end,
	
	
	receive
		{_} -> 
			
			RandomIndexInit = random:uniform(erlang:trunc(math:log(N)+1)),
			RandomNodeInit = lists:nth(RandomIndexInit, MyPartition),
			io:format("Starting gossip at ~p with ~p~n",[self(), RandomNodeInit]),
			io:format("Message : ~p~n~n",[{self(), send, avg, MyValues, MyPartition}]),
			RandomNodeInit ! {self(), send, avg, MyValues, MyPartition},
			
			gossip(MyPartition, N, MyValues, awaitingReply, RoundsRemaining - 1, 0);
			
		{init, Pids} ->
			MappingFun = fun(A) ->
						X=self(),
						Y = lists:nth(A,Pids),
						if Y == X ->
							lists:nth(random:uniform(N), Pids);
						true -> lists:nth(A,Pids)
						end
					end,
			%NewPartition = lists:map(fun(A) -> lists:nth(A, Pids) end, MyPartition),
			NewPartition = lists:map(MappingFun, MyPartition),
			io:format("~p initialised :~n Partition: ~p~n Values:~p~n~n", [self(), NewPartition, MyValues]),
			gossip(NewPartition, N, MyValues, initialised, RoundsRemaining, 0);
			
			

		{Node_id, send, Gossip_op, NodeValues, Node_partition} ->
			io:format("Received message : ~p~n", [{Node_id, send, Gossip_op, NodeValues, Node_partition}]),
			case Gossip_op of
				avg ->			
					%partition(MyPartition, Node_id, Node_partition, math:log(N),
					%io:format("~p In average send. ~n", [self()]),
					
				
					AveragingFunction = fun(NthValue) -> 
								%io:format("computing avg for ~p value",[NthValue]),								
								if 
									NthValue=<length(MyValues) , NthValue=<length(NodeValues) ->
										%io:format("averaging ~p'th values : ~p and ~p", [NthValue, lists:nth(NthValue,MyValues), lists:nth(NthValue,NodeValues)]),
										(lists:nth(NthValue,MyValues) + lists:nth(NthValue,NodeValues))/2;
									
									NthValue>length(MyValues), NthValue=<length(NodeValues) ->
										lists:nth(NthValue,NodeValues);
									NthValue=<length(MyValues) , NthValue>length(NodeValues)-> 
										lists:nth(NthValue,MyValues)
									end
								end,
					if
						length(NodeValues) =< length(MyValues) -> AveragingN = length(MyValues);
						true -> AveragingN=length(NodeValues)
					end,
					MyNewValues = lists:map(AveragingFunction, lists:seq(1, AveragingN)),
					io:format("~p new values: ~p~n", [self(),MyNewValues]),
					Node_id ! {self(), reply, avg, MyValues, MyPartition},
										
					gossip(MyPartition, N, MyNewValues, readyToSend, RoundsRemaining, 0)
			end;

		{Node_id, reply, Gossip_op, NodeValues, Node_partition} ->
			case Gossip_op of
				avg ->
					%partition(MyPartition, Node_id, Node_partition, math:log(N);
					%io:format("~p In average reply.~n", [self()])
					

					AveragingFunction = fun(NthValue) -> 
								%io:format("computing avg for ~p value",[NthValue]),								
								if 
									NthValue=<length(MyValues) , NthValue=<length(NodeValues) ->
										%io:format("averaging ~p'th values : ~p and ~p", [NthValue, lists:nth(NthValue,MyValues), lists:nth(NthValue,NodeValues)]),
										(lists:nth(NthValue,MyValues) + lists:nth(NthValue,NodeValues))/2;
									
									NthValue>length(MyValues), NthValue=<length(NodeValues) ->
										lists:nth(NthValue,NodeValues);
									NthValue=<length(MyValues) , NthValue>length(NodeValues)-> 
										lists:nth(NthValue,MyValues)
									
									end
								end,

					if
						length(NodeValues) =< length(MyValues) -> AveragingN = length(MyValues);
						true -> AveragingN=length(NodeValues)
					end,
					MyNewValues = lists:map(AveragingFunction, lists:seq(1, AveragingN)),
					io:format("~p new values: ~p~n", [self(),MyNewValues]),
					
										
					gossip(MyPartition, N, MyNewValues, readyToSend, RoundsRemaining, 0)	
					
			end;
		_ ->
			io:format("~p in Other shit~n", [self()])
	
	end	
.

init_the_dhondus(N) ->
	Values = lists:map(fun(_) -> random:uniform(100) end, lists:seq(0,N-1)),
	Pids = lists:map(fun(_) -> spawn(gossip_for_lists, gossip, [lists:map(fun(_) -> random:uniform(N) end, lists:seq(0,erlang:trunc(math:log(N)))), 
								N, 
								lists:map(fun(_)->random:uniform(100)+0.5 end, lists:seq(1, random:uniform(3))), 
								awaitingInitialisation, erlang:trunc(math:log(N) + 1), 0]) 
			end, 
		lists:seq(0,N-1)),

	io:format("~p~n",[Pids]),
	send_init(Pids, N),
	io:format("~n~nStarting ~p~n", [lists:nth(1,Pids)]),
	lists:nth(1,Pids) ! {1}
.

send_init(Pids, N) ->
	if N>0 -> lists:nth(N,Pids) ! {init,Pids},
		%io:format("dhondu ~p initialized~n", [lists:nth(N,Pids)]),
		send_init(Pids, N-1);
	true -> a
	end	
.



%gen_initial_partition(AgentNum, N, NodesGenerated, Partition) ->
%	
%	Node = random:uniform(N),
%	
%	if 
%		(NodesGenerated+1) < math:log(N) -> gen_initial_partition(AgentNum, N, NodesGenerated+1, dict:store(Node, 0, Partition));
%		
%		true -> dict.store(Node, 0, Partition)
%	end
%.
