package transferLearning.phases;

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

import transferLearning.dataTypes.XidAndSolId;
import transferLearning.dataTypes.SolIdAndSumError;
import transferLearning.dataTypes.YidAndSqrError;

public class Phase1Reducer implements Reducer<XidAndSolId, YidAndSqrError, IntWritable, SolIdAndSumError>,JobConfigurable {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(XidAndSolId arg0, Iterator<YidAndSqrError> it,
			OutputCollector<IntWritable, SolIdAndSumError> arg2, Reporter arg3)
			throws IOException {
		int xId = arg0.getxId();
		int solId = arg0.getSolId();
		double sumErr = 0;
		while (it.hasNext()){
			YidAndSqrError next = it.next();
			sumErr += next.getError();
		}
		arg2.collect(new IntWritable(xId), new SolIdAndSumError(solId, sumErr));
		
	}

	
}
