package codeBook.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class XClusterAndSumError implements WritableComparable<XClusterAndSumError> {
	int xCluster;
	double sumErr=1;
	
	
	public XClusterAndSumError() {
		
	}
	public XClusterAndSumError(int xc, double sumErr) {
		this.xCluster = xc;
		this.sumErr = sumErr;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		
		
		xCluster = in.readInt();
		sumErr = in.readDouble();
//		String line = in.readLine();
//		
//		System.out.println("reading: uc = " + uc + "\terr=" + sumErr );
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(xCluster);
		out.writeDouble(sumErr);
//		System.out.println("writing: uc = " + uc + "\terr=" + sumErr );
	}
	@Override
	public String toString() {
	
		return xCluster + "\t" + sumErr;
	}

	public double getSumErr() {
		return sumErr;
	}

	public int getXCluster() {
		return xCluster;
	}
	@Override
	public int compareTo(XClusterAndSumError o) {
		if (this.xCluster > o.xCluster){
			return 1;
		}else if(this.xCluster==o.xCluster){
			if (this.sumErr>o.sumErr){
				return 1;
			}else if(this.sumErr==o.sumErr){
				return 0;
			}else return -1;
		}else return -1;
	}

}
