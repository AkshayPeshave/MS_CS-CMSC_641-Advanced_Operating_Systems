-module(median).
-compile([debug_info, export_all]).

gossip(MyPartition, N, MyFragment, MyMedianComputation, Mode, RoundsRemaining, Delay) ->
	%if(length(FragmentsImFetching)>0)->
		%RoundsRemaining=lists:nth(3, FragmentsImFetching),
		%FragmentFetchStatus = lists:nth(2, FragmentsImFetching),
		if Mode == readyToSend, RoundsRemaining > 0, Delay==0 ->
			%timer:sleep(100),
			RandomIndex = random:uniform(length(MyPartition)),
			RandomNode = lists:nth(RandomIndex, MyPartition),
			
			if 
			RandomNode == self() -> 
				gossip(MyPartition, N, MyFragment, MyMedianComputation, readyToSend, RoundsRemaining-1, Delay);
			true ->			
				RandomNode ! {self(), median, send, MyMedianComputation},
				gossip(MyPartition, N, MyFragment, MyMedianComputation, awaitingReply, RoundsRemaining -1, Delay)
			end;
			
		Mode == readyToSend, RoundsRemaining == 0 ->
			io:format("Local Median Computation at ~p : ~p~n", [self(), (lists:nth(2, MyMedianComputation)+lists:nth(3, MyMedianComputation))/2]),
			exit("Fragment Received");		
		true -> a
		end,
	%true-> a
	%end,
	
	
	receive			
		{medianGossipStart, SendToId} -> 
			
				RandomIndexInit = random:uniform(length(MyPartition)),
				RandomNodeInit = lists:nth(RandomIndexInit, MyPartition),
				%io:format("Starting fetch fragment id ~p at ~p ~n",[FragmentId, self()]),
				%io:format("Message : ~p~n~n",[{self(), fetch, true, FragmentId}]),
				SendToId ! {self(), median, send, MyMedianComputation},
				
				gossip(MyPartition, N, MyFragment, MyMedianComputation, awaitingReply, RoundsRemaining, Delay);
			
			
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
			NewPartition = Pids,

			%NewPartition = lists:merge(lists:map(MappingFun, MyPartition), lists:map(fun(_)-> self() end, [self()])),
			io:format("~p initialising :~n Partition: ~p~n Fragment: ~p~n", [self(), NewPartition, MyFragment]),
			
			gossip(NewPartition, N, MyFragment, compute_local_median(MyMedianComputation,[]), initialised, RoundsRemaining, 0);
			
			

		% please reply with fragment or fetch for it amongst your neighbors
		{Node_id, median, send, NodesMedianComputation} ->
			if Mode == awaitingReply ->
				gossip(MyPartition, N, MyFragment, MyMedianComputation, awaitingReply, RoundsRemaining, Delay);
			true ->
				io:format("~p rcvd Active send from ~p: ~p~n", [self(),Node_id, [lists:nth(2, NodesMedianComputation),lists:nth(3, NodesMedianComputation), lists:nth(4, NodesMedianComputation)]]),
				Node_id ! {self(), median, reply, MyMedianComputation},
				gossip(MyPartition, N, MyFragment, compute_local_median(MyMedianComputation, NodesMedianComputation), readyToSend, RoundsRemaining, Delay-1)
			end;

		{Node_id, median, reply, NodesMedianComputation} ->
			io:format("~p rcvd Reply from ~p : ~p~n", [self(),Node_id, [lists:nth(2, NodesMedianComputation),lists:nth(3, NodesMedianComputation), lists:nth(4, NodesMedianComputation)]]),
			gossip(MyPartition, N, MyFragment, compute_local_median(MyMedianComputation, NodesMedianComputation), readyToSend, RoundsRemaining, 0);

		_ ->
			io:format("~p in Other shit~n", [self()])

		after 5000->
			if Mode==initialised; Mode==awaitingInitialisation ->
				gossip(MyPartition, N, MyFragment, MyMedianComputation, Mode, RoundsRemaining, 0);
			true->
				gossip(MyPartition, N, MyFragment, MyMedianComputation, readyToSend, 0, 0)
			
			end

	end	

.

compute_local_median(MyMedianComputation, NodesMedianComputation) ->
	if length(MyMedianComputation)==1 ->
		%io:format("Node ~p in medianComputation/initial~n", [self()]),
		
		SortedFragment = lists:nth(1, MyMedianComputation),
		NoOfValues = length(SortedFragment),
		if NoOfValues==1 ->
			%io:format("~p new median computation = ~p~n", [self(), lists:nth(1,SortedFragment)]),
			[SortedFragment, lists:nth(1, SortedFragment), lists:nth(1,SortedFragment),1];
		true -> 
			if (NoOfValues rem 2) == 0 ->		
				Median = (lists:nth(erlang:trunc(NoOfValues/2), SortedFragment) + lists:nth(erlang:trunc(1+(NoOfValues/2)), SortedFragment))/2,
				%io:format("median at ~p = ~p~n",[self(),Median]),
				MedianRepetitions = erlang:trunc((NoOfValues/2) + 1);
			true -> 
				Median = lists:nth(erlang:trunc((NoOfValues/2)+1), SortedFragment),
				MedianRepetitions = erlang:trunc((NoOfValues/2)+1)
			end,
			
			SumOfValues = compute_sum(SortedFragment, NoOfValues, 0),
			Average = SumOfValues/NoOfValues,
	
			if (NoOfValues == 2) ->
				NewFragment = lists:sort(lists:map(fun(_) -> Median end, lists:seq(1,MedianRepetitions)));
			true ->
				SumPreservationValue = (SumOfValues - (Median*MedianRepetitions)) / (NoOfValues - MedianRepetitions),
				NewFragment = lists:sort(lists:merge(lists:map(fun(_) -> Median end, lists:seq(1,MedianRepetitions)),
									lists:map(fun(_) -> %SumPreservationValue 
												%Average
												Median 
												end, lists:seq(1, NoOfValues - MedianRepetitions))
									)
							)
			end,

			%io:format("~p new median computation = ~p~n", [self(), Median]),
			%[NewFragment, Median, Average, length(MyMedianComputation)]
			[NewFragment, Median, SumOfValues, length(MyMedianComputation)]
		end;
	
	true ->
		
		SortedFragment = lists:sort(lists:merge(lists:nth(1, MyMedianComputation),lists:nth(1, NodesMedianComputation))),
		NoOfValues = length(SortedFragment),
		if (NoOfValues rem 2) == 0 ->		
			Median = (lists:nth(erlang:trunc(NoOfValues/2), SortedFragment) + lists:nth(erlang:trunc(1+(NoOfValues/2)), SortedFragment))/2;
			
		true -> 
			Median = lists:nth(erlang:trunc((NoOfValues/2)+1), SortedFragment)
			
		end,
		
		SumOfValues = compute_sum(SortedFragment, NoOfValues, 0),
		%SumOfValues = lists:nth(4,MyMedianComputation),
		Average = SumOfValues/NoOfValues,
		
				
		MyLength = lists:nth(4,MyMedianComputation),
		%MyLength= NoOfValues,
		MedianRepetitions = erlang:trunc((MyLength/2) + 1),

		if MyLength==1 ->
			 NewFragment = [Median];
		MyLength == 2 ->
			NewFragment = lists:sort(lists:map(fun(_) -> Median end, lists:seq(1,2)));
		true ->
			%SumPreservationValue = (SumOfValues - (Median*MedianRepetitions)) / (MyLength - MedianRepetitions),
			SumPreservationValue = (SumOfValues - (Median*MedianRepetitions)) / (MyLength - MedianRepetitions),
			NewFragment = lists:sort(lists:merge(lists:map(fun(_) -> Median end, lists:seq(1,MedianRepetitions)),
									lists:map(fun(_) -> %SumPreservationValue 
											%Average 
											Median 
											end, lists:seq(1, (MyLength - MedianRepetitions))
								)
						))
			end,

		io:format("~p new median computation = ~p~n", [self(), Median]),
		
		[NewFragment, Median, Average, MyLength]
	end
.

compute_sum(Values, Length, Sum) ->
	if Length >= 1 ->
		compute_sum(Values, Length-1, Sum+lists:nth(Length, Values));
	true -> Sum
	end
.

init_the_dhondus(N) ->
	%Values = lists:map(fun(_) -> random:uniform(100) end, lists:seq(0,N-1)),
	Fragments = lists:map(fun(_) -> lists:map(fun(_) -> random:uniform(1000)+0.5 end, lists:seq(1, 50)) end, lists:seq(0,N-1)),
	
	
	%FragmentsIdMap = lists:map(fun(_)-> random:uniform(erlang:trunc(N/2)) end, lists:seq(1,N)),
	Pids = lists:map(fun(Node) -> spawn(median, gossip, [lists:map(fun(_) -> random:uniform(N) end, lists:seq(0,erlang:trunc(math:log(N)))), 
								N, 
								lists:nth(Node, Fragments),  
								[lists:sort(lists:nth(Node, Fragments))],
								awaitingInitialisation, erlang:trunc(math:log(N)+1), 0]) 
			end, 
		lists:seq(1,N)),

	io:format("~p~n",[Pids]),
	send_init(Pids, N),
	%FetchFragment = random:uniform(N),
	%io:format("~n~n~p fetching fragment id ~p (fragment : ~p)~n", [lists:nth(1,Pids), lists:nth(1,FetchFragment), lists:nth(2,FetchFragment)]),
	timer:sleep(500),
	lists:nth(1,Pids) ! {medianGossipStart, lists:nth(length(Pids),Pids)},
	AllValues = all_values(Fragments, N, []),
	io:format("~n~nAll Values = ~p~n", [AllValues]),
	Median = compute_local_median([AllValues],[]),
	io:format("Median = ~p   Average=~p   Count=~p~n~n",[lists:nth(2,Median),lists:nth(3,Median),lists:nth(4,Median)])
.

send_init(Pids, N) ->
	if N>0 -> lists:nth(N,Pids) ! {init,Pids},
		%io:format("dhondu ~p initialized~n", [lists:nth(N,Pids)]),
		send_init(Pids, N-1);
	true -> io:format("stub done with initialisations~n~n~n")
	end	
.

all_values(Fragments, N, Values) ->
	if N>0 ->
		NewValues = lists:merge(Values, lists:nth(N, Fragments)),
		all_values(Fragments, N-1, NewValues);
	true ->
		Values
	end
.
