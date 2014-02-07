package codeBook.phases;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XIdAndClusters;
import codeBook.dataTypes.XidAndYCluster;

import utils.Utils;


public class Phase1Reducer implements Reducer<XidAndYCluster, IntWritable, XIdAndClusters, DoubleWritable>,JobConfigurable {

	String bUrl;
	int K,L;
	int bXCol, bYCol;
	double[][] B;
	
	@Override
	public void reduce(XidAndYCluster uidAndIc,
			Iterator<IntWritable> ratingIter,
			OutputCollector<XIdAndClusters, DoubleWritable> output, Reporter arg3)	
					throws IOException {
		
		int itemCluster = uidAndIc.getYCluster();
		int uid = uidAndIc.getUserId();
		double [] totalRate = new double[K];
		double [] countRates = new double[K]; 
		
		// go over all ratings with every possible user-cluster  
		// sum the rating and count them
		while (ratingIter.hasNext()) {
			IntWritable rating = ratingIter.next();
			int actualRating = rating.get();
			for(int i=0; i<K; i++){//possible user cluster
				totalRate[i] += actualRating;
				countRates[i]++;
			}
		}
		//for each possible user cluster we emit the average rating 
		for(int currUserCluster = 0; currUserCluster < K; currUserCluster++){
			double currentError = totalRate[currUserCluster]/countRates[currUserCluster];
			currentError -= B[currUserCluster][itemCluster];
			currentError = Math.abs(currentError);
			output.collect(new XIdAndClusters(uid, currUserCluster, itemCluster), new DoubleWritable(currentError));
		}
//		output.collect(new UserAndClusters(uid, 25, itemCluster), new DoubleWritable(0.1f));
	}

	@Override
	public void configure(JobConf arg0) {
		bUrl = arg0.get("bUrl");
		K = arg0.getInt("XclusterCount",20);
		L = arg0.getInt("YclusterCount",20);
		
		bXCol = arg0.getInt("bXCol", 0);
		bYCol = arg0.getInt("bYCol", 1);
		boolean isAfterUsers = arg0.getBoolean("isAfterUser", true);
		
		
		try{
			B = new double[K][L];
			Utils.getB(bUrl,K,L, B,bXCol, bYCol);
		}catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
