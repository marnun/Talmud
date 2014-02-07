package transferLearning.phases;

import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import codeBook.dataTypes.XClusterAndSumError;
import codeBook.dataTypes.XidAndYCluster;

import transferLearning.dataTypes.XidAndSolId;
import transferLearning.dataTypes.YidAndSqrError;
import utils.Utils;

public class Phase1BrutForceMapper implements Mapper<LongWritable, Text, XidAndSolId, YidAndSqrError> { 

	int K, L, N, bYCol, bXCol;
	double[][][] Bs;
	double[] alphas;
	private int xIdCol;
	private int yIdCol;

	@Override
	public void configure(JobConf arg0) {
		K = arg0.getInt("K",20);
		L = arg0.getInt("L",20);
		N = arg0.getInt("N",2);
		bXCol = arg0.getInt("bXCol",0);
		bYCol = arg0.getInt("bYCol",1);
		xIdCol = arg0.getInt("xIdCol",0);
		yIdCol = arg0.getInt("yIdCol",1);
		String bUrls = arg0.get("bUrl");
		String alphasUrl = arg0.get("alphasUrl");
		String[] bUrlArr = bUrls.split(",");
		try {
			loadCodebooks(bUrlArr);
			loadAlphas(alphasUrl);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	private void loadAlphas(String alphasUrl) throws IOException {
		alphas = Utils.loadAlphas(alphasUrl, N);
	}


	@Override
	public void close() throws IOException {}

	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<XidAndSolId, YidAndSqrError> output, Reporter arg3)
					throws IOException {
		int numSolutions = (int) Math.pow(K, N);
		String s = value.toString();
		String [] split = s.split("\t");
		if(split.length == 4){	
			try{
				int[] yClusters = new int[N];
				int xId = Integer.parseInt(split[xIdCol]);
				int yId = Integer.parseInt(split[yIdCol]);
				int rating = Integer.parseInt(split[3]);
				int ySolId = Integer.parseInt(split[2]);
				int num = ySolId;
				for (int n = 0; n < N; n++){
					yClusters[n] = num%L;
					num /= L;
				}
				
				for (int solId = 0; solId < numSolutions; solId++){
					int xCluster;
					num = solId;
					double prediction = 0;
					for (int n = 0; n < N; n++){
						xCluster = num%K;
						num /= K;
						prediction += alphas[n]*Bs[n][xCluster][yClusters[n]];
					}
					double sqrError = Math.pow(Math.abs(rating-prediction),2);
					try{
						XidAndSolId xidAndSolId = new XidAndSolId(xId, solId);
						YidAndSqrError yidAndSqrError = new YidAndSqrError(yId,sqrError);
						output.collect(xidAndSolId, yidAndSqrError);
					}catch (Exception e) {
						e.printStackTrace();
						System.out.println("error in spill -  ");
					}
				}
			}catch (Exception e){
				System.out.println("Failed to convert to required fields. line was: " + s );
				System.out.println("Exception was : " + e.getMessage());
//				e.printStackTrace();
			}
		}

	}

	public double[][][] loadCodebooks(String[] codebooksPaths) throws IOException {
		Bs = new double[codebooksPaths.length][][];
		for (int i = 0; i < codebooksPaths.length; i++) {
			Bs[i] = new double[K][L];
			Utils.getB(codebooksPaths[i], this.K,this.L, Bs[i], bXCol, bYCol);

		}		
		return Bs;
	}



}