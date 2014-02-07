package codeBook.phases;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XClusterAndSumError;

public class Phase3Reducer implements Reducer<IntWritable, XClusterAndSumError, IntWritable, XClusterAndSumError> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(IntWritable uid,
			Iterator<XClusterAndSumError> ucAndSumErrIter,
			OutputCollector<IntWritable, XClusterAndSumError> out, Reporter arg3)
					throws IOException {
		double minErr = Double.MAX_VALUE;
		int minXc = -1;
		while(ucAndSumErrIter.hasNext()){
			XClusterAndSumError current = ucAndSumErrIter.next();
			
			double currSumErr = current.getSumErr();
			if(minErr > currSumErr){
//				System.out.println("minErr > currSumErr -> " + minErr+ " > " + currSumErr );
				minErr = currSumErr;
				minXc = current.getXCluster();
				
			}

//			System.out.println(uid +"\tcurrUserCluster=" + current.toString() + "\tminUc = " + minXc);
		}
		//		System.out.println(uid + "\t" + minUc + "\tminerr=" +minErr);
		out.collect(uid, new XClusterAndSumError(minXc,minErr));
	}

}
