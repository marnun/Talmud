package transferLearning.phases;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import transferLearning.dataTypes.SolIdAndSumError;

public class Phase2FindMinErrMapper  implements Mapper<LongWritable, Text,  IntWritable, SolIdAndSumError>{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<IntWritable, SolIdAndSumError> output, Reporter arg3)
			throws IOException {
		String s = value.toString();
		String [] split = s.split("\t");
		if(split.length == 3){	
			try{
				int xId = Integer.parseInt(split[0]);
				int xSolId = Integer.parseInt(split[1]);
				double error = Double.parseDouble(split[2]);
				output.collect(new IntWritable(xId), new SolIdAndSumError(xSolId,error));
			}catch (Exception e) {
				e.printStackTrace();
				
			}
		}
	}
	

}
