-module(gossip_fetch_i).
-compile([debug_info, export_all]).

gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, Mode, Delay) ->
	if(length(FragmentsImFetching)>0)->
		RoundsRemaining=lists:nth(3, FragmentsImFetching),
		FragmentFetchStatus = lists:nth(2, FragmentsImFetching),
		if (Mode == readyToSend), (RoundsRemaining > 0) ->
			%timer:sleep(100),
			RandomIndex = random:uniform(length(MyPartition)),
			RandomNode = lists:nth(RandomIndex, MyPartition),
			
			%if RandomNode =:= self() ->
				%io:format("~p gossipping with ~p~n",[self(), RandomNode]),
				%io:format("Message : ~p~n~n",[{self(), send, avg, MyFragments, MyPartition}]),
				%RandomNode ! {self(), fetch, true, FragmentsImFetching},
				%gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, awaitingReply, RoundsRemaining - 1, 0)
	
				case lists:nth(2, FragmentsImFetching) of
					iAmFetching-> %{Node_id, fetch, true, FragmentNodeIsFetching}
						
						RandomNode ! {self(), fetch, true, lists:nth(1, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), iAmFetching, lists:nth(3, 	FragmentsImFetching)-1], awaitingReply, 0);

					fetch -> %{Node_id, fetch, true, FragmentNodeIsFetching}
						RandomNode !  {self(), fetch, true, lists:nth(1, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), fetch, lists:nth(3, FragmentsImFetching)-1], readyToSend, 0);

					disseminate -> %{Node_id, fetch, dataIsHere, FragmentNodeIsFetching, Fragments}
						RandomNode ! {self(), fetch, dataIsHere, lists:nth(1, FragmentsImFetching), lists:nth(4, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), disseminate, lists:nth(3, 	FragmentsImFetching)-1, lists:nth(4, FragmentsImFetching)], readyToSend, 0);
	
					false -> %{Node_id, fetch, false, FragmentNodeIsFetching}
						RandomNode ! {self(), fetch, false, lists:nth(1, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), false, lists:nth(3, 	FragmentsImFetching)-1, lists:nth(4, FragmentsImFetching)], readyToSend, 0);
	
					myFragment -> %{Node_id, fetch, dataIsHere, FragmentNodeIsFetching, Fragments}
						RandomNode ! {self(), fetch, dataIsHere, MyFragmentIds, MyFragments},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), myfragment, lists:nth(3, 	FragmentsImFetching)-1], readyToSend, 0);
	
					fragmentReceived -> %{Node_id, fetch, false, FragmentNodeIsFetching}
						RandomNode ! {self(), fetch, false, lists:nth(1, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), fragmentReceived, lists:nth(3, 	FragmentsImFetching)-1, lists:nth(4, FragmentsImFetching)], readyToSend, 0);
	
					timeout -> %%{Node_id, fetch, false, FragmentNodeIsFetching}			
						RandomNode ! {self(), fetch, false, lists:nth(1, FragmentsImFetching)},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), timeout, lists:nth(3, 	FragmentsImFetching)-1], readyToSend, 0)
				end;
			%true -> gossip(MyPartition, N, MyFragments, MyFragmentIds, [lists:nth(1, FragmentsImFetching), iAmFetching, lists:nth(3, 	FragmentsImFetching)], readyToSend, 0)
			%timer:sleep(200)
			%end;
			
		Mode == readyToSend, RoundsRemaining == 0 ->
			if 
				FragmentFetchStatus == fragmentReceived -> 
					io:format("Fragment Received : ~p~n", [lists:nth(4, FragmentsImFetching )]),
					exit("Fragment Received");
	
				true -> a
					%io:format("done gossiping....~p exiting~n", [self()])
			end;
		
		true -> a
		end;
	true-> a
	end,
	
	
	receive
		{Node, tellFragmentId} ->
			Node ! {MyFragmentIds, MyFragments};
			
		{fetchStart, FragmentId, SendToId} -> 
			if MyFragmentIds == FragmentId ->
				io:format("I (~p) own fragment id ~p~n....exiting~n",[self(), FragmentId]),
				exit("self owned fragment");
			true->
				RandomIndexInit = random:uniform(length(MyPartition)),
				RandomNodeInit = lists:nth(RandomIndexInit, MyPartition),
				%io:format("Starting fetch fragment id ~p at ~p ~n",[FragmentId, self()]),
				%io:format("Message : ~p~n~n",[{self(), fetch, true, FragmentId}]),
				SendToId ! {self(), fetch, true, FragmentId},
				
				gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentId, iAmFetching, length(MyPartition)*3], awaitingReply, 0)
			end;
			
		{init, Pids} ->
			MappingFun = fun(A) ->
						X=self(),
						Y = lists:nth(A,Pids),
						if Y == X ->
							lists:nth(random:uniform(N), Pids);
						true -> lists:nth(A,Pids)
						end
					end,
			NewPartition = lists:map(fun(A) -> lists:nth(A, Pids) end, MyPartition),
			%NewPartition = lists:merge(lists:map(MappingFun, MyPartition), lists:map(fun(_)-> self() end, [self()])),
			%io:format("~p initialised :~n Partition: ~p~n FragmentId: ~p~n Values:~p~n~n", [self(), NewPartition, MyFragmentIds, MyFragments]),
			gossip(NewPartition, N, MyFragments, MyFragmentIds, [], initialised, 0);
			
			

		% please reply with fragment or fetch for it amongst your neighbors
		{Node_id, fetch, true, FragmentNodeIsFetching} ->
			%io:format("I am ~p....Received message : ~p~n", [self(),{Node_id, fetch, true, FragmentNodeIsFetching}]),

			% I don't have this fragment. Need to disseminate request for this fragment.
			if length(FragmentsImFetching)==0 ->
				
				if FragmentNodeIsFetching == MyFragmentIds ->
					%io:format("in fetch_true/0length/myData"),
					Node_id ! {self(), fetch, dataIsHere, FragmentNodeIsFetching, MyFragments},
					gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, myfragment, length(MyPartition)*3], readyToSend, 0);

				true ->
					%io:format("in fetch_true/0length/startFetch"),
					gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, fetch, length(MyPartition)*3], readyToSend, 0)
				end;
			true->
				%io:format("in fetch_true/true"),
				FragmentIdUnderway = lists:nth(1,FragmentsImFetching),
				FragmentStatusUnderway = lists:nth(2, FragmentsImFetching),
	
				case FragmentNodeIsFetching of
					% I have this fragment and I have to disseminate this fragment.				
					MyFragmentIds -> 
						 
						Node_id ! {self(), fetch, dataIsHere, FragmentNodeIsFetching, 	MyFragments},						
						gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);
					
					
					%it's the fragment i am fetching
					FragmentIdUnderway ->
						
						if 
							%the fragment has already been received by fetching process
							FragmentStatusUnderway == fragmentReceived; FragmentStatusUnderway == false -> 
								Node_id ! {self(), fetch, false, FragmentNodeIsFetching},
								gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);
	
							% i am still waiting for the fragment to be fetched for me
							FragmentStatusUnderway == iAmFetching ->
								gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);
	
							% i am disseminating the fragment to be fetched as it has been found
							FragmentStatusUnderway == disseminate ->
								Node_id ! {self(), fetch, dataIsHere, FragmentNodeIsFetching, lists:nth(4, FragmentsImFetching)},
								gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);
							
							true ->
								gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0)
						end;
						
						
					
					_ -> 
						gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0)
				end		
			end;

		% stop fetching for this fragment and disseminate the same message
		{Node_id, fetch, false, FragmentNodeIsFetching} ->
			%io:format("I am ~p....Received message : ~p~n", [self(),{Node_id, fetch, false, FragmentNodeIsFetching}]),
			
			if 
				length(FragmentsImFetching)==0 ->
					gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, false, length(MyPartition)*3], readyToSend, 0);		
				true -> 
					FragmentStatusUnderway = lists:nth(2, FragmentsImFetching),
	
					if FragmentStatusUnderway =:= false ->			
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, false, length(MyPartition)*3], readyToSend, 0);

					true ->
						gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0)
					end
			end;
			

		% here is the data someone is fetching...please disseminate
		{Node_id, fetch, dataIsHere, FragmentNodeIsFetching, Fragments} ->
			%io:format("I am ~p....Received message : ~p~n", [self(),{Node_id, fetch, dataIsHere, FragmentNodeIsFetching, Fragments}]),
			if 
				length(FragmentsImFetching)==0 ->
					gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, disseminate, length(MyPartition)*3, Fragments], readyToSend, 0);
				
				true ->
					FragmentIdUnderway = lists:nth(1,FragmentsImFetching),
					FragmentStatusUnderway = lists:nth(2, FragmentsImFetching),

					if (FragmentIdUnderway == FragmentNodeIsFetching), (FragmentStatusUnderway == iAmFetching) ->
						io:format("Fragment received : ~p",[Fragments]),
						Node_id ! {self(), fetch, false, FragmentNodeIsFetching},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, fragmentReceived, length(MyPartition)*3, Fragments], readyToSend, 0);

					(FragmentIdUnderway == FragmentNodeIsFetching), (FragmentStatusUnderway == fragmentReceived) ->
						Node_id ! {self(), fetch, false, FragmentNodeIsFetching},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);

					% need to consider case of interim node which will forward the data as well as the one stopping mode
					(FragmentIdUnderway == FragmentNodeIsFetching), (FragmentStatusUnderway == fetch) ->
						gossip(MyPartition, N, MyFragments, MyFragmentIds, [FragmentNodeIsFetching, disseminate, length(MyPartition)*3, Fragments], readyToSend, 0);
				
					(FragmentIdUnderway == FragmentNodeIsFetching), (FragmentStatusUnderway == false) ->
						Node_id ! {self(), fetch, false, FragmentNodeIsFetching},
						gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0);
	
					true -> gossip(MyPartition, N, MyFragments, MyFragmentIds, FragmentsImFetching, readyToSend, 0)

					end
			end;


		_ ->
			io:format("~p in Other shit~n", [self()])

	end	

.

average_final_values(FinalValues, Length, Index, Sum) ->
	if Index =< Length ->
		average_final_values(FinalValues, Length, Index+1, Sum+lists:nth(Index, FinalValues));
	true -> Sum/Length
	end
.

init_the_dhondus(N) ->
	Values = lists:map(fun(_) -> random:uniform(100) end, lists:seq(0,N-1)),
	Fragments = lists:map(fun(_) -> lists:map(fun(_) -> random:uniform(100)+0.5 end, lists:seq(1, random:uniform(3))) end, lists:seq(0,erlang:trunc(N/2))),
	FragmentsIdMap = lists:map(fun(_)-> random:uniform(erlang:trunc(N/2)) end, lists:seq(1,N)),
	Pids = lists:map(fun(Node) -> spawn(gossip_fetch_i, gossip, [lists:map(fun(_) -> random:uniform(N) end, lists:seq(0,erlang:trunc(math:log(N)))), 
								N, 
								lists:nth(lists:nth(Node, FragmentsIdMap), Fragments), 
								lists:nth(Node, FragmentsIdMap), 
								[],
								awaitingInitialisation, 0]) 
			end, 
		lists:seq(1,N)),

	io:format("~p~n",[Pids]),
	FetchFragment = send_init(Pids, N),
	%FetchFragment = random:uniform(N),
	io:format("~n~n~p fetching fragment id ~p (fragment : ~p)~n", [lists:nth(1,Pids), lists:nth(1,FetchFragment), lists:nth(2,FetchFragment)]),
	lists:nth(1,Pids) ! {fetchStart, lists:nth(1,FetchFragment), lists:nth(length(Pids),Pids)}
.

send_init(Pids, N) ->
	if N>0 -> lists:nth(N,Pids) ! {init,Pids},
		%io:format("dhondu ~p initialized~n", [lists:nth(N,Pids)]),
		send_init(Pids, N-1);
	true -> 
		lists:nth(random:uniform(length(Pids)), Pids) ! {self(), tellFragmentId},
		receive
			{FragmentId, Fragment} -> [FragmentId,Fragment]
		end
	end	
.
