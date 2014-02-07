package codeBook.phases;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XIdAndCluster;
import codeBook.dataTypes.YClusterAndError;

public class Phase2Mapper implements Mapper<LongWritable, Text, XIdAndCluster, YClusterAndError> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text stringVal,
			OutputCollector<XIdAndCluster, YClusterAndError> out,
			Reporter arg3) throws IOException {
		String[] values = stringVal.toString().split("\t");
		int uid = Integer.parseInt(values[0]);
		int uc = Integer.parseInt(values[1]);
		int ic = Integer.parseInt(values[2]);
		double err = Double.parseDouble(values[3].split("\n")[0]);
		out.collect(new XIdAndCluster(uid, uc),new YClusterAndError(ic, err));		
	}
	
}
