package transferLearning.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SolIdAndSumError implements WritableComparable<SolIdAndSumError> {

	
	int solId;
	double sumError;
	
	public SolIdAndSumError() {
		// TODO Auto-generated constructor stub
	}
	
	public SolIdAndSumError(int solId, double sumError) {
		super();
		this.solId = solId;
		this.sumError = sumError;
	}
	public int getSolId() {
		return solId;
	}
	public void setSolId(int solId) {
		this.solId = solId;
	}
	public double getSumError() {
		return sumError;
	}
	public void setSumError(double sumError) {
		this.sumError = sumError;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		solId = arg0.readInt();
		sumError = arg0.readDouble();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(solId);
		arg0.writeDouble(sumError);		
	}
	@Override
	public int compareTo(SolIdAndSumError o) {
		if (solId > o.solId){
			return 1;
		}if (solId < o.solId){
			return -1;
		}
		return 0;
	}
	@Override
	public String toString() {
	
		return solId+ "\t" + sumError;
	}
	
}
