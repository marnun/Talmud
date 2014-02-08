Talmud
======

Talmud is a codeBook transfer learning algorithm from N domains to a new domain

this project is made to work with the Hadoop 2.0.4 -intel distribution of the apache hadoop framework.

it needs 2 more external libreries: apache-math, and pig.


in order to run it, there are 2 steps:
1. calculate codebooks to all n domains. this will done by calling the codeBook.Main class, for every domain:
these are the parameters needed to run for every domain codebook:
<K> <L> <start_iteration> <epsilon> <maxIterations> <TraindataPath> <ValidationdataPath> <seperator> <prefix>
where K is the number of users clusters
L is the number of items clusters
start_iteration: should be 1 by default, but it is possible to resume this program from this iteration
epsilon: will effect the stop condition.
maxIterations : will also effect the stop condition. this represent the maximum number of iterations in the algorithm
TraindataPath : HDFS location of the train set
ValidationdataPath: HDFS location of the validation set
seperator: is the column seperator used in the train and validation sets.
prefix: HDFS location for the data.


2. transfer the learning to the new domain. this will be done by calling the transferLearning.MainTL class with the folowing arguments:
<prefix> <trainDataPath> <validationDataPath> <seperator> <schema> <K> <L> <codebooksPaths> <B_User_column> <B_Item_column> <epsilon><maxIterations>
where 
prefix: HDFS location for the data.
trainDataPath: HDFS location of the train set for the new domain
ValidationdataPath: HDFS location of the validation set for the new domin
seperator: is the column seperator used in the train and validation sets.
schema is the schema for the train and validation sets. schema should look like: "uid:int,iid:int,rating:int,unixtime:int"
K is the number of users clusters
L is the number of items clusters
codebooksPaths should be comma seperated values, (i.e. data/b0/,data/b1/)
B_User_column : the users clusters column in all given codebooks.
B_Item_column : the items clusters column in all given codebooks.
epsilon: will effect the stop condition.
maxIterations : will also effect the stop condition. this represents the maximum number of iterations in the algorithm
