
-- generate the codebook B
fullJoin = load 'initFullJoin'as (uid:int, uc:int, iid:int, ic:int, rating:int )

tmp = GROUP fullJoin by (uc,ic);
B = foreach tmp generate group, AVG(fullJoin.rating) as avgRating;
STORE B INTO 'initCodeBook';

-- these tables are written to the hdfs in the following format:
--initCodeBook : (userCluster:int, itemCluster:int, avgRating:double)
--initFullJoin : (userid:int, movieid:int, userCluster:int, movieCluster:int, rating:int);
--initV: (itemId:int,itemCluster:int)
--initU: (userId:int, userCluster:int)
