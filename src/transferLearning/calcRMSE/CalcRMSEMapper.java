package transferLearning.calcRMSE;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import utils.Utils;

public class CalcRMSEMapper implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	int K, L, N ;
	double[][][] Bs;
	double[] alphas;
	final IntWritable zero = new IntWritable(0);
	
	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<IntWritable, DoubleWritable> out, Reporter arg3)
			throws IOException {
		String s = value.toString();
		String [] split = s.split("\t");
		if(split.length >= 5){	
			try{
				int[] itemClusters = new int[N];
				int[] userClusters = new int[N];
				int userSolId = Integer.parseInt(split[2]);
				int itemSolId = Integer.parseInt(split[3]);
				int rating = Integer.parseInt(split[4]);
				int itemNum = itemSolId;
				int userNum = userSolId;
				for (int n = 0; n < N; n++){
					itemClusters[n] = itemNum%L;
					userClusters[n] = userNum%K;
					itemNum /= L;
					userNum /= K;
				}
				double pred1 =0;
				for (int i = 0; i < N; i++){
					pred1 += alphas[i]*Bs[i][userClusters[i]][itemClusters[i]];
				}
				out.collect(zero, new DoubleWritable(Math.abs(rating - pred1)));
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	} 
	
	@Override
	public void configure(JobConf arg0) {
		K = arg0.getInt("K",20);
		L = arg0.getInt("L",20);
		N = arg0.getInt("N",2);
		String alphasUrl = arg0.get("alphasUrl");
		String bUrls = arg0.get("bUrl");
		String[] bUrlArr = bUrls.split(",");
		try {
			loadCodebooks(bUrlArr);
			alphas = Utils.loadAlphas(alphasUrl, N);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public double[][][] loadCodebooks(String[] codebooksPaths) throws IOException {
		Bs = new double[codebooksPaths.length][][];
		for (int i = 0; i < codebooksPaths.length; i++) {
			Bs[i] = new double[K][L];
			Utils.getB(codebooksPaths[i], this.K,this.L, Bs[i], 1, 0);
		}		
		return Bs;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
