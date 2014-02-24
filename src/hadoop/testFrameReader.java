package hadoop;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.BytesWritable;

public class testFrameReader {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		InputStream in;
		try {
			in = new FileInputStream("/Users/damak/Desktop");
			OldFrameReader fr = new OldFrameReader(in,10000);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("wili wili");
		}
				
		//BytesWritables frame = new BytesWritable();
		
		//readFrame( frame, 10000, 10000); 
                
		
		
		
		
		
		
		
		
		
	}

}
