Graph = LOAD '$G' USING PigStorage(',') AS ( a:long, b:long );

grouped1 = group Graph by a;

count1 = FOREACH grouped1 GENERATE group,COUNT(Graph);

grouped2 = group count1 by $1;

count2 = FOREACH grouped2 GENERATE group,COUNT($1);

STORE count2 INTO '$O' USING PigStorage (',');