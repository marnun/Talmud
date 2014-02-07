package transferLearning.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class XidAndSolId implements WritableComparable<XidAndSolId> {

	public XidAndSolId() {
		// TODO Auto-generated constructor stub
	}
	public int getxId() {
		return xId;
	}

	public void setxId(int xId) {
		this.xId = xId;
	}

	public int getSolId() {
		return solId;
	}

	public void setSolId(int solId) {
		this.solId = solId;
	}

	int xId, solId;
	public XidAndSolId(int xId, int solId) {
		super();
		this.xId = xId;
		this.solId = solId;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		try {
			xId = arg0.readInt();
			solId = arg0.readInt();
		} catch (Exception e) {
		
			e.printStackTrace();
		}
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(xId);
		arg0.writeInt(solId);
		
	}


	@Override
	public int compareTo(XidAndSolId o) {
		if (solId > o.solId){
			return 1;
		}
		if (solId== o.solId){
			if (xId>o.xId){
				return 1;
			}
			if (xId == o.xId){
				return 0;
			}
			return -1;
		}
		return -1;
	}
	
	@Override
	public String toString() {
	
		return xId + "\t" + solId;
	}

}
