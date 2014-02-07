/*init.pig
initialy load data to pig and generate tables U and V with random clusters assigned for each user and each item */

-- set number of reducers
SET default_parallel 20


%default K '20'; --number of users clusters 
%default L '20'; --number of items clusters

--the load method to load the data from the original files of movielans
data = LOAD 'ml-100k/u.data' AS ( userid:int, movieid:int, rating:int, unixtime:int); 


--creating the original U and V vectors.
tmp = GROUP data by userid;
U = foreach tmp generate group as userid,group%$K as userCluster;
STORE U INTO 'initU';
tmp1 = GROUP data by movieid;
V = foreach tmp1 generate group as movieid ,group%$L as movieCluster;
STORE V INTO 'initV';

--generating the full join 
tmp2 = JOIN data BY userid, U BY userid;
tmp3 = JOIN tmp2 BY movieid, V BY movieid;
fullJoin = foreach tmp3 GENERATE data::userid AS uid, data::movieid AS iid , U::userCluster AS uc, V::movieCluster AS ic, data::rating AS rating;
STORE fullJoin INTO 'initfullJoin';

--load fullJoin
--fullJoin = LOAD 'initfullJoin' as (userid:int, movieid:int, userCluster:int, movieCluster:int, rating:int);



