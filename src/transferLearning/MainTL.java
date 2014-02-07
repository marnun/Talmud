package transferLearning;


import java.io.IOException;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import transferLearning.calcRMSE.CalcRMSEMapper;
import transferLearning.calcRMSE.CalcRMSEReducer;
import transferLearning.dataTypes.SolIdAndSumError;
import transferLearning.dataTypes.XidAndSolId;
import transferLearning.dataTypes.YidAndSqrError;
import transferLearning.learningAlphas.CodeBookPair;
import transferLearning.learningAlphas.PreprocessDataForMatrixMulLeftMapper;
import transferLearning.learningAlphas.PreprocessDataForMatrixMulRightMapper;
import transferLearning.learningAlphas.PreprocesssDataForMatrixMulLeftReducer;
import transferLearning.learningAlphas.PreprocesssDataForMatrixMulRightReducer;
import transferLearning.phases.Phase1BrutForceMapper;
import transferLearning.phases.Phase2FindMinErrMapper;
import utils.Utils;

public class MainTL {
	private String trainDataPath;
	private String validationDataPath;
	private char seperator;
	int vColIndex;
	String initialSchema;
	int bXCol, bYCol;
	int L, K;
	double[][][] codebooks;
	int N;
	String alphasPath;
	String prefix;
	String[] codeBookPaths;
	private String alphasFullJoin;
	String bUrl;
	private double epsilon;
	private int maxIterations;
	public MainTL(String prefix, String trainDataPath, String validationDataPath, char seperator, int vColIndex,
			String initialSchema, int k,int l, String[] codebooksPaths,int bXCol, int bYCol, double epsilon,int maxIterations) throws Exception {
		super();
		this.codeBookPaths = codebooksPaths;
		this.trainDataPath = trainDataPath;
		this.validationDataPath = validationDataPath;
		this.seperator = seperator;
		this.vColIndex = vColIndex;
		this.initialSchema = initialSchema;
		this.L = l;
		this.K = k;
		this.bXCol = bXCol;
		this.bYCol = bYCol;
		N = codebooksPaths.length;
		alphasPath = Utils.initializeAlphas(prefix, N);
		this.prefix = prefix;
		Utils.initializeV(prefix, trainDataPath, seperator, vColIndex, initialSchema, L, N, "V_0");
		bUrl = getBurl();
		this.maxIterations = maxIterations;
		this.epsilon = epsilon;

	}



	private void run() throws Exception{


		int t = 1;
		double oldRmse = Double.MAX_VALUE;
		double rmse = 0.0;
		while (Math.abs(oldRmse-rmse)>epsilon && t<maxIterations){
			/**
			 * generate a join between the original data, table U and table V 
			 */
			String fullJoinPath = generateFullJoin(t,true);
			/**
			 * phase 1:
			 * input : <userId, itemId, UsolNumer, rating>
			 * output:
			 * this will be the solId with the SumError for every solution:
			 * <uid, uSolNumber, sumError>
			 * 
			 */
			String inputDir = prefix + fullJoinPath;
			String outputDir = prefix + "phase1X_" +t;

			JobConf conf = genPhase1Conf(inputDir, outputDir,0,1);
			runJob(t, conf);


			/**
			 * phase 2 
			 * this will get the output of phase1 and will find solution with the minimum sumError
			 * <uid,solNumber, sumError>  
			 */
			inputDir = outputDir;
			String currTJobName = "U_" +t;
			outputDir = prefix + currTJobName;


			updateConfPhase2(inputDir, outputDir, conf, currTJobName);
			runJob(t, conf);


			//do the same for items
			bXCol = 1;
			bYCol = 0;
			fullJoinPath = generateFullJoin(t,false);

			/**phase 1a
			 * same as phase 1, this time for items
			 */
			inputDir = prefix + fullJoinPath;
			outputDir = prefix + "phase1Y_" +t;
			conf = genPhase1Conf(inputDir, outputDir,1,0);

			runJob(t, conf);

			/**phase 2a
			 * same as phase 2, this time for items
			 */
			inputDir = outputDir;
			currTJobName = "V_" +t;
			outputDir = prefix + currTJobName;
			updateConfPhase2(inputDir, outputDir, conf, currTJobName);
			runJob(t, conf);

			/**
			 * this will calculate the matrixes for calculating the alphas.
			 */
			alphasPath = learnAlphas(t);
			oldRmse = rmse;
			rmse = getRMSE(t);
			t++;

		}		


	}



	private String getBurl() {
		StringBuilder sb = new StringBuilder(codeBookPaths[0]);
		for (int i = 1; i < codeBookPaths.length; i++) {
			sb.append("," +codeBookPaths[i]);
		}

		String bUrl = sb.toString();
		return bUrl;
	}



	private double getRMSE(int t) throws IOException {
		/**
		 * generating the full join pre-process for alphas learning
		 * this process will generate the file:
		 * <uid, iid, uSolNumber ,iSolNumber, rating> 
		 */
		String input = Utils.generateFullJoinForPreprocessForAlphasLearning(t, validationDataPath, initialSchema, seperator, prefix,true);
		String output = prefix + "RMSE_" + t;
		JobConf conf = new JobConf(MainTL.class);

		conf.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		/**
		 * this mapper will get the full join file, and will output:
		 *  for each row in the file '0' -> <abs(prediction-rating)>
		 */
		conf.setMapperClass(CalcRMSEMapper.class);
		
		/**
		 * this reduce will return the RMSE of all mapper output. as the RMSR
		 */
		conf.setReducerClass(CalcRMSEReducer.class);

		conf.setMapOutputKeyClass(IntWritable.class );
		conf.setMapOutputValueClass(DoubleWritable.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setInt("K",K);
		conf.setInt("L",L);
		conf.setInt("N",N);
		conf.set("bUrl",bUrl);
		conf.set("alphasUrl",alphasPath);
		runJob(t, conf);
		return Utils.readRMSEFromFile(output);
	}


	private String learnAlphas(int t) throws IOException {
		/**
		 * generating the full join pre-process for alphas learning
		 * this process will generate the file:
		 * <uid, iid, uSolNumber ,iSolNumber, rating> 
		 */
		alphasFullJoin = Utils.generateFullJoinForPreprocessForAlphasLearning(t, trainDataPath, initialSchema, seperator, prefix,false);
		//calgulate the left side of the equation (the big matrix)
		String leftMatrixPath = prefix + "alphasPh1Left_" + t; 
		JobConf conf = new JobConf(MainTL.class);

		conf.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(conf, new Path(alphasFullJoin));
		FileOutputFormat.setOutputPath(conf, new Path(leftMatrixPath));

		conf.setMapperClass(PreprocessDataForMatrixMulLeftMapper.class);
		conf.setReducerClass(PreprocesssDataForMatrixMulLeftReducer.class);

		conf.setMapOutputKeyClass(CodeBookPair.class );
		conf.setMapOutputValueClass(DoubleWritable.class);

		conf.setOutputKeyClass(CodeBookPair.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setInt("K",K);
		conf.setInt("L",L);
		conf.setInt("N",N);
		conf.set("bUrl",bUrl);
		runJob(t, conf);

		//calgulate the right side of the equation (the vector
		String rightMatrixPath = prefix + "alphasPh1Right_" + t;

		FileInputFormat.setInputPaths(conf, new Path(alphasFullJoin));
		FileOutputFormat.setOutputPath(conf, new Path(rightMatrixPath));

		conf.setMapperClass(PreprocessDataForMatrixMulRightMapper.class);
		conf.setReducerClass(PreprocesssDataForMatrixMulRightReducer.class);

		conf.setMapOutputKeyClass(IntWritable.class );
		conf.setMapOutputValueClass(DoubleWritable.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		runJob(t, conf);

		//mutiple the matrixes
		double[][] leftMatrix = Utils.getMatrixFromFile(leftMatrixPath, N, N);

		for (int i = 0; i < N; i++){
			for (int j = i; j < N; j++){
				leftMatrix[j][i] = leftMatrix[i][j];  
			}
		}
		RealMatrix leftInvers = Utils.inversMatrix(leftMatrix);
		double[][] rightMatrix = Utils.getMatrixFromFile(rightMatrixPath, N, 1);
		RealMatrix right = MatrixUtils.createRealMatrix(rightMatrix);	
		RealMatrix multiply = leftInvers.multiply(right);
		double[][] mulMatrix = multiply.getData();
		alphasPath = Utils.saveMatrixToFile(prefix,"alphas_" + t, mulMatrix);
		return alphasPath;
	}







	private void updateConfPhase2(String inputDir, String outputDir, JobConf conf,
			String currTJobName) {
		conf.setJobName( currTJobName);
		FileInputFormat.setInputPaths(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		conf.setMapperClass(Phase2FindMinErrMapper.class);//(Phase1Mapper.class);
		conf.setReducerClass(transferLearning.phases.Phase2Reducer.class);
		conf.setMapOutputKeyClass(IntWritable.class );
		conf.setMapOutputValueClass(SolIdAndSumError.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(SolIdAndSumError.class);
	}



	private JobConf genPhase1Conf(String inputDir, String outputDir, int xIdCol, int yIdCol) {
		JobConf conf = new JobConf(MainTL.class);

		conf.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));

		conf.setMapperClass(Phase1BrutForceMapper.class);//(Phase1Mapper.class);
		conf.setReducerClass(transferLearning.phases.Phase1Reducer.class);

		conf.setMapOutputKeyClass(XidAndSolId.class );
		conf.setMapOutputValueClass(YidAndSqrError.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(SolIdAndSumError.class);

		conf.setInt("K",K);
		conf.setInt("L",L);
		conf.setInt("N",N);
		conf.setInt("bXCol",bXCol);
		conf.setInt("bYCol",bYCol);
		conf.setInt("xIdCol",xIdCol);
		conf.setInt("yIdCol",yIdCol);
		conf.set("bUrl",bUrl);
		conf.set("alphasUrl",alphasPath);
		return conf;
	}



	private void runJob(int t, JobConf conf) {
		try {
			RunningJob runJob = JobClient.runJob(conf);
			while (!runJob.isComplete()){
				try{
					System.out.println("sleeping..." + t );
					Thread.sleep(500);

				}catch (InterruptedException e) {}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String generateFullJoin(int t, boolean isUserJoin) throws IOException {
		return Utils.generateFullJoin(t,this.trainDataPath,this.initialSchema, this.seperator, this.prefix, isUserJoin);
	}



	public static void main(String[] args) throws Exception{


		if (args.length < 13){
			System.err.println("");
			System.exit(-1);
		}
			
			
		//deleteOldFiles();
		//args[0];

		String prefix = "data/";//args[0];
		String trainDataPath = "u.data";//args[1];
		String validationDataPath = "u.data";//args[2];
		if (args[3].length() != 1){
			System.err.println("the seperator can be only of size 1, valus such \t, \n should be in a quoats: '\t' ");
			System.exit(-1);
		}
		char seperator = '\t'; //args[3].charAt(0);
		int vColIndex = 1;//Integer.parseInt(c);
		String schema = "uid:int,iid:int,rating:int,unixtime:int";//args[5]
		
		int K = 20;//Integer.parseInt(args[6]);
		int L = 20;//Integer.parseInt(args[7]);
		String[] codebooksPaths = new String[]{"data/b0/","data/b1/"};//args[8].split(",");
		
		int bUCol = 0;//Integer.parsInt(args[9]); // this represent the user column in the codebooks files.
		int bVCol = 1;//Integer.parsInt(args[10]); // this represent the items column in the codebooks files.
		double epsilon = 0.1;//Double.parseDouble(args[11]);
		int maxIterations = 20;//Integer.parsInt(args[12]); // this represent the maximum number of iterations in the algorithm 
		
		MainTL mainTL = new MainTL(prefix,trainDataPath,validationDataPath, seperator, vColIndex , schema, K, L, codebooksPaths, bUCol, bVCol, epsilon,maxIterations);
		mainTL.run();
	}



	private static void deleteOldFiles() throws IOException {
		Path[] pt = {};//{new Path("./data/phase1X_1" ),new Path("./data/fullJoin_1"),new Path("./data/U_1"),new Path("./data/V_0"),new Path("./data/alphas_0")};
		FileSystem fs = FileSystem.get(new Configuration());
		for (Path path : pt) {
			fs.delete(path, true);	
		}


	}
}
