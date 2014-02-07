package codeBook;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class CodeBookBuilder {

	
	/**
	 * this method will calculate the new codebook for a certain iteration
	 * @param prefix
	 * @param trainDataPath the location of the training data
	 * @param k number of user clusters
	 * @param l number of item clusters
	 * @param iteration
	 * @param seperator for loading the data
	 * @param isAfterUser if true, will not return RMSE
	 * @param validationDataPath the location of the validation set.
	 * @return the if iteration>0 rmse else Double.MAX_VALUE
	 * @throws IOException
	 */
	public double buildCodeBook(String prefix, String trainDataPath, int k, int l, int iteration, String seperator, boolean isAfterUser, String validationDataPath) throws IOException{
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
//		pigServer.setDefaultParallel(k);
		
		pigServer.registerQuery("data = LOAD '" + trainDataPath + "' USING PigStorage('" + seperator + "') AS ( userid:int, movieid:int, rating:int, unixtime:int);");
		if (iteration < 1){ //should create a random U and V tables
			//creating the original U and V vectors.
			pigServer.registerQuery("tmp = GROUP data by userid;");
			
			if (isAfterUser){
				pigServer.registerQuery("U = LOAD '" + prefix +"U_" + (iteration+1) + "' AS (userid:int,userCluster:int, err:float);");
				pigServer.registerQuery("V = LOAD '" + prefix +"V_" + (iteration) + "' AS (movieid:int,movieCluster, err:float);");
				
			}else{
				pigServer.registerQuery("U = foreach tmp generate group as userid,group%" + k + 
						" as userCluster;");
				pigServer.store("U", prefix + "U_"+iteration);

				pigServer.registerQuery("tmp1 = GROUP data by movieid;");
				pigServer.registerQuery("V = foreach tmp1 generate group as movieid ,group%" + l + 
						" as movieCluster;");
				pigServer.store("V", prefix+"V_" + iteration);
			}
			

		}else{
			if (isAfterUser){
				pigServer.registerQuery("U = LOAD '" + prefix +"U_" + (iteration+1) + "' AS (userid:int,userCluster:int, err:float);");
			}else{
				pigServer.registerQuery("U = LOAD '" + prefix +"U_" + (iteration) + "' AS (userid:int,userCluster:int, err:float);");
			}
			pigServer.registerQuery("V = LOAD '" + prefix +"V_" + (iteration) + "' AS (movieid:int,movieCluster, err:float);");
			pigServer.registerQuery("U = FOREACH U GENERATE userid,userCluster;");
			pigServer.registerQuery("V = FOREACH V GENERATE movieid,movieCluster;");
		}

		pigServer.registerQuery("tmp2 = JOIN data BY userid, U BY userid;");
		pigServer.registerQuery("tmp3 = JOIN tmp2 BY movieid, V BY movieid;");
		pigServer.registerQuery("fullJoin = foreach tmp3 GENERATE data::userid AS uid, data::movieid AS iid , U::userCluster AS uc, V::movieCluster AS ic, data::rating AS rating;");
		if (isAfterUser){
			pigServer.store("fullJoin",prefix + "fullJoin_U_"+iteration);
		}else{
			pigServer.store("fullJoin",prefix + "fullJoin_"+iteration);
		}
		pigServer.shutdown();
		//build the actual Codebook
		pigServer = new PigServer(ExecType.MAPREDUCE);
//		pigServer.setDefaultParallel(1);
		if (isAfterUser){
			pigServer.registerQuery("fullJoin = LOAD '" + prefix +"fullJoin_U_"+iteration + 
					"'as (uid:int, iid:int, uc:int, ic:int, rating:int );");
		}else{
			pigServer.registerQuery("fullJoin = LOAD '" + prefix +"fullJoin_"+iteration + 
				"'as (uid:int, iid:int, uc:int, ic:int, rating:int );");
		}
		pigServer.registerQuery("tmp = GROUP fullJoin by (uc,ic);");
		pigServer.registerQuery("B = foreach tmp generate group.uc, group.ic , AVG(fullJoin.rating) as avgRating;");
		if (isAfterUser){
			pigServer.store("B", prefix + "CodeBook_U_" + iteration	);
		}else{
			pigServer.store("B", prefix + "CodeBook_" + iteration);
		}
		if (iteration < 1|| isAfterUser){
			return Float.MAX_VALUE;
			
		}else{
			pigServer.registerQuery("B = LOAD '" + prefix + "CodeBook_" + iteration + "' USING PigStorage('" + seperator + "') AS ( userCluster:int, movieCluster:int, prediction:double);");
			pigServer.registerQuery("data = LOAD '" + validationDataPath + "' USING PigStorage('" + seperator + "') AS ( userid:int, movieid:int, rating:int, unixtime:int);");
			pigServer.registerQuery("U = LOAD '" + prefix +"U_" + (iteration) + "' AS (userid:int,userCluster:int,err:float);");
			pigServer.registerQuery("V = LOAD '" + prefix +"V_" + (iteration) + "' AS (movieid:int,movieCluster,err:float);");
			pigServer.registerQuery("tmp2 = JOIN data BY userid, U BY userid;");
			pigServer.registerQuery("tmp3 = JOIN tmp2 BY movieid, V BY movieid;");
			pigServer.registerQuery("tmp4 = JOIN tmp3 BY (U::userCluster,V::movieCluster), B BY (userCluster,movieCluster);");
			pigServer.registerQuery("fullJoin = foreach tmp4 GENERATE data::userid AS uid, data::movieid AS iid , U::userCluster AS uc, V::movieCluster AS ic, (ABS(data::rating-B::prediction)*ABS(data::rating-B::prediction)) AS sqrErr;");
			pigServer.registerQuery("sumAll = group fullJoin ALL;");
			pigServer.registerQuery("sumAll = foreach sumAll generate COUNT(fullJoin.sqrErr) as count, SUM(fullJoin.sqrErr) as sum;");
			Iterator<Tuple> it = pigServer.openIterator("sumAll");
			Double sumErr = new Double(0);
			Long count = new Long(0);
			if (it.hasNext()){
				Tuple next = it.next();
				count = (Long)next.get(0);
				sumErr = (Double)next.get(1);
			}
			double sumSqError = sumErr.doubleValue();
			
			
			
			pigServer.shutdown();
			return Math.sqrt(sumSqError/(count));
		}
		

	}


	
}
