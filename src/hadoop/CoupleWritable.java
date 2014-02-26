package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CoupleWritable implements Writable{

	private double frameDate;
	private double paramVal;// it can also be String or List
	
	public CoupleWritable(){
	}
	
	public CoupleWritable(double frameDate,double paramVal){
		this.setFrameDate(frameDate);
		this.setParamVal(paramVal);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(frameDate);
		out.writeDouble(paramVal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		frameDate= in.readDouble();
		paramVal= in.readDouble();
	}

	public double getFrameDate() {
		return frameDate;
	}

	public void setFrameDate(double frameDate) {
		this.frameDate = frameDate;
	}

	public double getParamVal() {
		return paramVal;
	}

	public void setParamVal(double paramVal) {
		this.paramVal = paramVal;
	}

}
