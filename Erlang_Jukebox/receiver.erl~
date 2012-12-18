-module(receiver).
-export([listen/1]).

listen(N) -> 
	 register(listenerProc,spawn(fun()->
					receive	
						_Other-> 
							io:format("Received a message : "),
							unregister(listenerProc),
							listen(0)
					end
				end)).
