package codeBook.phases;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XClusterAndSumError;

public class Phase3Mapper implements Mapper<LongWritable, Text, IntWritable, XClusterAndSumError> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text val,
			OutputCollector<IntWritable, XClusterAndSumError> out,
			Reporter arg3) throws IOException {
		String[] values = val.toString().split("\t");
		int uid = Integer.parseInt(values[0]);
		int uc = Integer.parseInt(values[1]);
		double sumErr = Double.parseDouble(values[2].split("\n")[0]);
//		System.out.println(uid + "\t" + uc + "\t" + sumErr);
		out.collect(new IntWritable(uid), new XClusterAndSumError(uc, sumErr));
		
	}

}
