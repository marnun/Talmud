package transferLearning.dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.python.antlr.PythonParser.yield_expr_return;

public class YidAndSqrError implements WritableComparable<YidAndSqrError> {

	int yId;
	double error;
	 
	public YidAndSqrError() {
		// TODO Auto-generated constructor stub
	}
	
	public int getyId() {
		return yId;
	}

	public void setyId(int yId) {
		this.yId = yId;
	}

	public double getError() {
		return error;
	}

	public void setError(double error) {
		this.error = error;
	}

	public YidAndSqrError(int yId, double error) {
		super();
		this.yId = yId;
		this.error = error;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		yId = arg0.readInt();
		error = arg0.readDouble();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(yId);
		arg0.writeDouble(error);
		
	}

	@Override
	public int compareTo(YidAndSqrError o) {
		if (yId > o.yId){
			return 1;
		}
		if (yId == o.yId){
			return 0;
		}
		return -1;
	}

}
