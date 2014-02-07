package codeBook.phases;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XIdAndCluster;
import codeBook.dataTypes.YClusterAndError;

public class Phase2Reducer implements Reducer<XIdAndCluster, YClusterAndError, XIdAndCluster, DoubleWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(XIdAndCluster uidAndUc,
			Iterator<YClusterAndError> IcsAndErrs,
			OutputCollector<XIdAndCluster, DoubleWritable> out, Reporter arg3)
					throws IOException {
		double sumErr = 0;
		while(IcsAndErrs.hasNext()){
			sumErr += IcsAndErrs.next().getErr();
		}
		out.collect(uidAndUc, new DoubleWritable(sumErr));
	}



}
