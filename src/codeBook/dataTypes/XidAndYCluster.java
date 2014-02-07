package codeBook.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class XidAndYCluster implements WritableComparable<XidAndYCluster> {
	public XidAndYCluster() {
	// TODO Auto-generated constructor stub
	}
	private int xId;
	private int yCluster;
//	private String error;
	
	public int getUserId(){
		return xId;
	}
	
	public int getYCluster(){
		return yCluster;
	}
	
	public XidAndYCluster (int id, int cluster){
		xId = id;
		yCluster = cluster;
//		error = "";
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		xId = in.readInt();
		yCluster = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(xId);
		out.writeInt(yCluster);
//		out.writeBytes(error);
	}
	
	@Override
	public int compareTo(XidAndYCluster o) {
		if (this.xId>o.xId){
			return 1;
			
		}
		if (this.xId==o.xId){
			if (this.yCluster>o.yCluster){
				return 1;
			}else if(this.yCluster==o.yCluster){
				return 0;
			}else return -1;
		}
		else return -1;
		
		
	}
}
