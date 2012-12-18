-module(forLoopTemplate).
-export([for/1]).


for(0) ->
	0
;

for(NoOfIterations) when is_integer(NoOfIterations)-> 
	for(NoOfIterations-1),
	NoOfIterations
.



