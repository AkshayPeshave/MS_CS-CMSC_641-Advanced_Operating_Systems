-module(node_partitioner).
-export([partition/4, choose_random_node/1, gossip/2, init_the_dhondu/0]).

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

gossip(MyPartition, N) ->
	
	receive
		[Node_id, "send", Gossip_op, Value, Node_partition] ->
			case gossip_op of
				"avg" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				"min" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				"max" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				

			end

		[node_id, "reply", gossip_op, value, node_partition] ->
			case gossip_op of
				"avg" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				"min" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				"max" ->
					partition(MyPartition, Node_id, Node_partition, math:log(N);

				
			end
	
	end	
.


gen_initial_partition(AgentNum, N, NodesGenerated, Partition)
	
	Node = random:uniform(N),
	
	if 
		(NodesGenerated+1) < math:log(N) -> gen_initial_partition(AgentNum, N, NodesGenerated+1,r dict.store(Node, 0, Partition));
		
		true -> dict.store(Node, 0, Partition)
	end
.


init_the_dhondus(AgentNum, N) ->
	register(gossip
	MyPartition = gen_initial_partition(AgentNum, N, 0, dict:new(),

	register(activeThread, spawn(gossip(InitialPartition, N))),
	register(passiveThread, spawn(passive_thread)),
	register(queueService, spawn(receive_on_active_queue))
.
