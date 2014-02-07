package transferLearning.learningAlphas;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import utils.Utils;

public class PreprocessDataForMatrixMulLeftMapper implements Mapper<LongWritable, Text, CodeBookPair, DoubleWritable> {

	int K, L, N ;
	double[][][] Bs;

	
	@Override
	public void configure(JobConf arg0) {
		K = arg0.getInt("K",20);
		L = arg0.getInt("L",20);
		N = arg0.getInt("N",2);
		String bUrls = arg0.get("bUrl");
		String[] bUrlArr = bUrls.split(",");
		try {
			loadCodebooks(bUrlArr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<CodeBookPair, DoubleWritable> out, Reporter arg3)
			throws IOException {
		String s = value.toString();
		String [] split = s.split("\t");
		if(split.length >= 4){	
			try{
				int[] itemClusters = new int[N];
				int[] userClusters = new int[N];
				int xId = Integer.parseInt(split[0]);
				int yId = Integer.parseInt(split[1]);
				int userSolId = Integer.parseInt(split[2]);
				int itemSolId = Integer.parseInt(split[3]);
				int itemNum = itemSolId;
				int userNum = userSolId;
				for (int n = 0; n < N; n++){
					itemClusters[n] = itemNum%L;
					userClusters[n] = userNum%K;
					itemNum /= L;
					userNum /= K;
				}
				
				for (int i = 0; i < N; i++){
					double pred1 = Bs[i][userClusters[i]][itemClusters[i]];
					for (int j = i; j < N; j++){
						double pred2 = Bs[j][userClusters[j]][itemClusters[j]];
						out.collect(new CodeBookPair(i, j), new DoubleWritable(pred1 * pred2));					
					}
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
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

	
}
