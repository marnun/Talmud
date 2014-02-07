package transferLearning.phases;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import transferLearning.dataTypes.SolIdAndSumError;

public class Phase2Reducer implements Reducer<IntWritable, SolIdAndSumError, IntWritable, SolIdAndSumError>,JobConfigurable {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(IntWritable xId, Iterator<SolIdAndSumError> it,
			OutputCollector<IntWritable, SolIdAndSumError> output, Reporter arg3)
			throws IOException {
		
		double minError = Double.MAX_VALUE;
		SolIdAndSumError minSolId = null;
		while (it.hasNext()){
			SolIdAndSumError currSolution = it.next();
			double currSumError = currSolution.getSumError();
			if (minError>currSumError){
				minError = currSumError;
				minSolId = currSolution;
			}
		}
		output.collect(xId, minSolId);
		
	}


	
}
