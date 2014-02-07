package codeBook.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class XIdAndClusters implements WritableComparable<XIdAndClusters> {

	private int xId;
	private int xCluster;
	private int yCluster;

	
	public XIdAndClusters(int uid, int uc, int ic) {
		xId = uid;
		xCluster = uc;
		this.yCluster = ic;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		xId = in.readInt();
		xCluster = in.readInt();
		yCluster = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(xId);
		out.writeInt(xCluster);
		out.writeInt(yCluster);
	}


	
	@Override
	public String toString() {
		return xId + "\t" + xCluster + "\t" + yCluster;
//		return super.toString();
	}

	@Override
	public int compareTo(XIdAndClusters o) {
		if (this.xId>o.xId){
			return 1;
			
		}
		if (this.xId==o.xId){
			if (this.xCluster>o.xCluster){
				return 1;
			}else if(this.xCluster==o.xCluster){
				if (this.yCluster>o.yCluster){
					return 1;
				}else if (this.yCluster==o.yCluster){
					return 0;
				}else return -1;
			}else return -1;
		}
		else return -1;
		
		
	}
	
}
