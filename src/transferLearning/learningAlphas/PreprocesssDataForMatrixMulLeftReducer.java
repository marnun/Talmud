package transferLearning.learningAlphas;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PreprocesssDataForMatrixMulLeftReducer implements Reducer<CodeBookPair, DoubleWritable, CodeBookPair, DoubleWritable>,JobConfigurable {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(CodeBookPair key, Iterator<DoubleWritable> it,
			OutputCollector<CodeBookPair, DoubleWritable> out, Reporter arg3)
			throws IOException {
		double sum = 0;
		while (it.hasNext()){
			sum += it.next().get();
		}
		
		out.collect(key, new DoubleWritable(sum));
		
	}

}
