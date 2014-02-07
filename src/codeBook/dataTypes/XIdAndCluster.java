package codeBook.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class XIdAndCluster implements WritableComparable<XIdAndCluster> {

	private int xid;
	private int xCluster;
	
	public XIdAndCluster(){
		
	}
	public XIdAndCluster(int uid,int uc){
		this.xCluster = uc;
		this.xid = uid;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		xid = in.readInt();
		xCluster = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(xid);
		out.writeInt(xCluster);
	}
	@Override
	public String toString() {
	
		return xid + "\t" + xCluster;
	}

	public long getUid() {
		return xid;
	}
	public void setXid(int xid) {
		this.xid = xid;
	}
	public int getXc() {
		return xCluster;
	}
	public void setXc(int xc) {
		this.xCluster = xc;
	}
	
	@Override
	public int compareTo(XIdAndCluster o) {
		if (this.xid>o.xid){
			return 1;
			
		}
		if (this.xid==o.xid){
			if (this.xCluster>o.xCluster){
				return 1;
			}else if(this.xCluster==o.xCluster){
				return 0;
			}else return -1;
		}
		else return -1;
		
		
	}
	
}
