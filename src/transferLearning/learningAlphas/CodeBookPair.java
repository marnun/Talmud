package transferLearning.learningAlphas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CodeBookPair implements WritableComparable<CodeBookPair> {

	int cb1,cb2;
	public CodeBookPair() {
		// TODO Auto-generated constructor stub
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		cb1 = arg0.readInt();
		cb2 = arg0.readInt();
				
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(cb1);
		arg0.writeInt(cb2);
		
	}
	@Override
	public int compareTo(CodeBookPair o) {
		if (cb1 < o.cb1) {
			return -1;
		}
		if (cb1 == o.cb1){
			if (cb2 < o.cb2){
				return -1;
			}
			if (cb2==o.cb2){
				return 0;
			}
		}
		return 1;
	}
	
	public int getCb1() {
		return cb1;
	}
	public void setCb1(int cb1) {
		this.cb1 = cb1;
	}
	public int getCb2() {
		return cb2;
	}
	public void setCb2(int cb2) {
		this.cb2 = cb2;
	}
	public CodeBookPair(int cb1, int cb2) {
		super();
		this.cb1 = cb1;
		this.cb2 = cb2;
	}

	@Override
	public String toString() {
	
		return cb1 + "\t" + cb2;
	}
	
	
}
