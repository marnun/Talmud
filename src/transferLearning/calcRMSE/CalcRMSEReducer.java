package transferLearning.calcRMSE;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalcRMSEReducer implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>,JobConfigurable {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(IntWritable key, Iterator<DoubleWritable> it,
			OutputCollector<IntWritable, DoubleWritable> out, Reporter arg3)
			throws IOException {
		int count = 0;
		double sum = 0;
		
		while (it.hasNext()){
			double error = it.next().get();
			sum += Math.pow(error,2);
			count++;
		}
		
		double rmse = Math.sqrt(sum/count);
		out.collect(new IntWritable(0), new DoubleWritable(rmse));
		
	}

}
