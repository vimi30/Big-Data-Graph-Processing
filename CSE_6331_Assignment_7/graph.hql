drop table Graph;

create table Graph(a int , b int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Graph;

INSERT OVERWRITE TABLE Graph select a, count(*) from Graph group by a;

SELECT b,count(*) from Graph group by b;