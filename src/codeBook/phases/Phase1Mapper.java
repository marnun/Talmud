package codeBook.phases;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import codeBook.dataTypes.XidAndYCluster;
//the first phase of mar reduce:
//group data like this: <userId, itemCluster> -> <item, rating>
//input: <line number, line text>
public class Phase1Mapper implements Mapper<LongWritable, Text, XidAndYCluster, IntWritable> {


	@Override
	public void map(LongWritable key, Text value, OutputCollector<XidAndYCluster, IntWritable> output,
			Reporter reporter) throws IOException {
		//get the current line
		
		String s = value.toString();
		String [] split = s.split("\t");
		if(split.length == 5){	
			try{
				int xId = Integer.parseInt(split[xIdCol]);
				int yCluster = Integer.parseInt(split[yClusterCol]);
				int yId = Integer.parseInt(split[yIdCol]);
				int rating = Integer.parseInt(split[4].split("\n")[0]);//remove the last "\n" if exists
				output.collect(new XidAndYCluster(xId,yCluster), 
						new IntWritable(rating));
			}
			catch (Exception e){
				System.out.println("Failed to convert to required fields. line was: " + s);
			}
		}
	}


	int xIdCol;
	int yIdCol;
	int xClusterCol;
	int yClusterCol;
	

//	@Override
public void configure(JobConf arg0) {
	xIdCol = arg0.getInt("xIdCol", 0);
	yIdCol = arg0.getInt("yIdCol", 1);
	xClusterCol = arg0.getInt("xClusterCol", 2);
	yClusterCol = arg0.getInt("yClusterCol", 3);

}

//	@Override
public void close() throws IOException {
	// TODO Auto-generated method stub

}



}
