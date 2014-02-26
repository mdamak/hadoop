package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CoupleWritable implements Writable{

	private double frameDate;
	private String paramVal;// double and list of doubles will be represented as String
	private int paramSize;  
	public CoupleWritable(){
	}
	
	public CoupleWritable(double frameDate,String paramVal){
		paramSize= paramVal.length();
		this.setFrameDate(frameDate);
		this.setParamVal(paramVal);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(frameDate);
		out.writeChars(paramVal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		frameDate= in.readDouble();
		for (int i=0; i<paramSize; i++)
			paramVal+= in.readChar();
	}

	public double getFrameDate() {
		return frameDate;
	}

	public void setFrameDate(double frameDate) {
		this.frameDate = frameDate;
	}

	public String getParamVal() {
		return paramVal;
	}

	public void setParamVal(String paramVal) {
		this.paramVal = paramVal;
	}

	public int getParamSize() {
		return paramSize;
	}

	public void setParamSize(int paramSize) {
		this.paramSize = paramSize;
	}

}
