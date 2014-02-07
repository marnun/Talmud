package codeBook;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

import codeBook.phases.RunPhase;





public class Main extends Configured implements Tool{
	public static void main(String[] args) {
		if (args.length<12){
			System.out.println("Usage: <phase number(1,2,3)> " +
					"<input dir> <output dir> <codebook url> " +
					"<K> <L> <start_iteration> <epsilon> <maxIterations>" +
					"<TraindataPath> <ValidationdataPath> <seperator> <prefix>");
		}
		int K=Integer.parseInt(args[4]);
		int L=Integer.parseInt(args[5]);
		int iteration = 1;//Integer.parseInt(args[6]);
		double epsilon = Double.parseDouble(args[7]);
		int maxIterations = Integer.parseInt(args[8]);
		
		ArrayList<Double> oldRMSEs = new ArrayList<Double>();

		CodeBookBuilder cbb = new CodeBookBuilder();
		
		try {
			String trainDataPath = args[9];
			String validationDataPath = args[10];
			String seperator = args[11];
			String prefix = args[12];
			double rmse = cbb.buildCodeBook(prefix, trainDataPath, K, L, iteration, seperator,false,trainDataPath) ;
			double prevRmse = 0;

			
			while (Math.abs(rmse-prevRmse) > epsilon && iteration <= maxIterations){
				oldRMSEs.add(rmse);
				
				writeResultToFile(iteration, prefix, rmse);
				
				args[3] = prefix + "CodeBook_" + iteration + "/part-r-00000";
				args[1] = prefix + "fullJoin_" + iteration;
				String input = args[1];
				//runing on users

				RunPhase runner = new RunPhase();
				JobConf conf = runner.getConf();
				conf.setInt("xIdCol", 0);
				conf.setInt("yIdCol", 1);
				conf.setInt("xClusterCol", 2);
				conf.setInt("yClusterCol", 3);
				conf.setBoolean("isAfterUser", false);

				conf.setInt("bXCol", 0);
				conf.setInt("bYCol", 1);
				conf.setInt("XclusterCount", K);
				conf.setInt("YclusterCount", L);
				String bUrl = args[3];
				conf.set("bUrl", bUrl);

				String[] phase1Args =null;
				String[] phase2Args =null;
				String[] phase3Args =null;
				
				phase1Args = args;
				phase1Args[0] = "1";
				phase1Args[1] = input;
				phase1Args[2]= prefix + "ph1X_" + iteration;
				System.out.println(phase1Args[2] + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase1Args));
				/**
				 * Phase 1
				 * input : <userId,itemId,userCluster,itemCluster,rating>
				 * the mapper will generate:
				 * <userId, itemCluster> => <rating>
				 * the reducer will emit:
				 * <userId, userCluster, itemCluster> => <avgError = avg(rating)-B[userCluster][itemCluster]>
				 * 
				 */
				runner.run(phase1Args);

				phase2Args = args;
				phase2Args[1]= phase2Args[2];
				phase2Args[2]= prefix + "ph2X_" + iteration;
				phase2Args[0] = "2";
				System.out.println(phase2Args[2]+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase2Args));
				/**
				 * Phase 2
				 * input :  <userId, userCluster, itemCluster, avgError>
				 * the mapper will generate:
				 *  <userId, userCluster> => <itemCluster, avgError>
				 * the reducer will emit:
				 * <userId, userCluster> => <sumError = sum(avgError)>
				 * 
				 */
				runner.run(phase2Args);


				phase3Args = args;
				phase3Args[0] = "3";
				phase3Args[1]= phase2Args[2];
				phase3Args[2]= prefix + "U_"+(iteration+1);
				System.out.println(phase3Args[2] + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase3Args));

				/**
				 * Phase 3
				 * input :  <userId, userCluster, sumError>,
				 * the mapper will generate:
				 *  <userId, userCluster> => <sumError>
				 * the reducer will emit:
				 * <userId> => <userCluster> , Min(sumError)>
				 * notice, this reducer will return a single userCluster - the one with the minimum sumError.
				 * 
				 */
				runner.run(phase3Args);
				
				
				/**
				 * this will requild the CodeBook
				 */
				cbb.buildCodeBook(prefix, trainDataPath, K, L, iteration,seperator,true,trainDataPath) ;
				conf.set("bUrl",prefix + "CodeBook_U_" + iteration + "/part-r-00000" );

				//!!!!running on items

				conf.setInt("xIdCol", 1);
				conf.setInt("yIdCol", 0);
				conf.setInt("xClusterCol", 3);
				conf.setInt("yClusterCol", 2);
				conf.setInt("bXCol", 1);
				conf.setInt("bYCol", 0);
				conf.setInt("XclusterCount", L);
				conf.setInt("YclusterCount", K);

				phase1Args[0] = "1";
				phase1Args[1]= prefix + "fullJoin_U_" + iteration;;
				phase1Args[2]= prefix + "ph1Y_" + iteration;
				System.out.println(phase1Args[2] + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase1Args));

				/**
				 * same as phase 1, this time for items
				 */
				runner.run(phase1Args);

				phase2Args = args;
				phase2Args[0] = "2";
				phase2Args[1]= phase2Args[2];
				phase2Args[2]= prefix + "ph2Y_" + iteration;
				System.out.println(phase2Args[2]+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase2Args));
				/**
				 * same as phase 2, this time for items
				 */
				runner.run(phase2Args);

				phase3Args = args;
				phase3Args[1]= phase3Args[2];
				phase3Args[2]= prefix + "V_" + (iteration+1);
				phase3Args[0] = "3";
				System.out.println(phase3Args[2]+ "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println(Arrays.toString(phase3Args));

				/**
				 * same as phase 3, this time for items
				 */
				runner.run(phase3Args);
				iteration++;
				prevRmse = rmse;
				/**
				 * rebuilding the codebook, and returning the new RMSE on the ValidationSet
				 */
				rmse = cbb.buildCodeBook(prefix, trainDataPath, K, L, iteration,seperator, false, validationDataPath) ;
				System.out.println("iteration " + (iteration) + " completed with RMSE= " +rmse+"\t#@!#@!#@!#@!#!@#@!#@!#!@#@! ");
				
			}
			oldRMSEs.add(rmse);
			writeResultToFile(iteration, prefix, rmse);
			System.out.println("all RMSEs :" + oldRMSEs.toString());
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	
	/**
	 * this will write the RMSE and the iteration number to a file.
	 * @param iteration
	 * @param prefix
	 * @param rmse
	 * @throws IOException
	 */
	private static void writeResultToFile(int iteration, String prefix,
			double rmse) throws IOException {
		OutputStreamWriter sw = creatResultFile(prefix);
		BufferedWriter br=new BufferedWriter(sw);
		br.append("iteation: "+iteration + " RMSE: " + rmse+"\n");
		br.flush();
		br.close();
		sw.close();
	}

	private static OutputStreamWriter creatResultFile(String prefix)
			throws IOException {
		Path pt=new Path(prefix +"results.out");
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream file = null;//fs.append(pt);
		if (!fs.createNewFile(pt)){
			file = fs.append(pt);
		}else{
			file = fs.create(pt);
		}
		
		OutputStreamWriter out = new OutputStreamWriter(file);
		return out;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Main.main(arg0);
		return 0;
	}






}
