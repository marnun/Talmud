package codeBook.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class YClusterAndError implements Writable {

	private int yCluster;
	private double err;
	
	public YClusterAndError(){
		
	}
	
	
	public YClusterAndError(int ic, double err){
		this.yCluster = ic;
		this.err = err;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		yCluster = in.readInt();
		err = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(yCluster);
		out.writeDouble(err);

	}

	public double getErr() {
		return err;
	}
	
	@Override
	public String toString() {
	
		return yCluster + "\t" + err;
	}

}
