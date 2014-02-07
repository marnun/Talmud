package codeBook.phases;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import codeBook.dataTypes.XClusterAndSumError;
import codeBook.dataTypes.XIdAndCluster;
import codeBook.dataTypes.XIdAndClusters;
import codeBook.dataTypes.XidAndYCluster;
import codeBook.dataTypes.YClusterAndError;

public class RunPhase {//extends Configured implements Tool{

	JobConf conf;
	


	public RunPhase() {
		conf = new JobConf(/*getConf(),*/ RunPhase.class);
	}


	public RunningJob run(String[] args){
		//TODO: basically the same as "WordCount"

		int phaseNum=0;
		if (args.length < 7) {
			System.out.println(
					"Usage: <phase number(1,2,3)> <input dir> <output dir> <codebook url><K><L><iteration><epsilon><maxIterations>\n");
			return null;
		}
		try{
			phaseNum = Integer.parseInt(args[0]);
		}
		catch(Exception e){
			System.out.println("first argument must be phase number (1,2,3)");
			return null;
		}
		if(phaseNum>3||phaseNum<1){
			System.out.println("phase number may be one of these: (1,2,3)");
			return null;
		}
		
		
		int iteration = Integer.parseInt(args[6]);
		
		
		String inputDir = args[1];
		String outputDir = args[2];

		
		conf.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));

		switch (phaseNum){
		case 1: //phase1
			conf.setMapperClass(Phase1Mapper.class);//(Phase1Mapper.class);
			conf.setReducerClass(Phase1Reducer.class);

			conf.setMapOutputKeyClass(XidAndYCluster.class);
			conf.setMapOutputValueClass(IntWritable.class);


			conf.setOutputKeyClass(XIdAndClusters.class);
			conf.setOutputValueClass(DoubleWritable.class);

			break;
		case 2://phase2
			conf.setMapperClass(Phase2Mapper.class);
			conf.setReducerClass(Phase2Reducer.class);

			conf.setMapOutputKeyClass(XIdAndCluster.class);
			conf.setMapOutputValueClass(YClusterAndError.class);
			
			conf.setOutputKeyClass(XIdAndCluster.class);
			conf.setOutputValueClass(DoubleWritable.class);
			break;
		case 3://phase3
			conf.setMapperClass(Phase3Mapper.class);
			conf.setReducerClass(Phase3Reducer.class);

			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(XClusterAndSumError.class);

			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(XClusterAndSumError.class);
			break;
		}
		try {
			RunningJob runJob = JobClient.runJob(conf);
			while (!runJob.isComplete()){
				try{
					System.out.println("sleeping..." + iteration );
					Thread.sleep(500);
				
				}catch (InterruptedException e) {}
			}
			return runJob;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public JobConf getConf() {
		return conf;
	}


	public void setConf(JobConf conf) {
		this.conf = conf;
	}
}

