package hadoop;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;

public class testHadoop {

	public static void main(String[] args) throws IOException {

     	
     	////trame 1/////
		//resultqt attendu : 0|0|0|0|0|0|0|1|1|2|0|0|0|4
     	//consumed bytes : 10
     	ByteBuffer bf1 = ByteBuffer.allocate(10);
     	bf1.put((byte) 0);bf1.put((byte) 7);bf1.put((byte) 1);
		bf1.put((byte) 1);bf1.put((byte) 2);bf1.put((byte) 0);bf1.put((byte) 3);bf1.put((byte) 4);bf1.putShort((byte) 0);
		byte[] trame1 = bf1.array();
		/////////////////
		
		
		
		////trame 2/////
		//resultqt attendu : 0|0|0|0|0|0|0|1|2|
     	//consumed bytes : 6
     	ByteBuffer bf2 = ByteBuffer.allocate(6);
     	bf2.put((byte) 0);bf2.put((byte) 7);bf2.put((byte) 1);
     	bf2.put((byte) 2); bf2.putShort((byte) 0);
		byte[] trame2 = bf2.array();
		/////////////////
				
		
		FileOutputStream fos = new FileOutputStream("/Users/Hamza/Desktop/trameTest.bin");
		fos.write(trame1);
		fos.write(trame2);
		fos.close();
		
		InputStream in = new FileInputStream("/Users/Hamza/Desktop/trameTest.bin");
		FrameReader fr = new FrameReader(in,4); 
		
	/**	Random rg = new Random(3);
 
		for (int i=0;i<4;i++){
			System.out.println(rg.nextDouble());
		} */	


		////test de readFrame
		///il faut utiliser copybytes pour recuperer la value de BytesWritable
		BytesWritable bw =new BytesWritable();
		for (int i=0;i<2;i++){
		  int res = fr.readFrame(bw,0, 0,10000000);
		  System.out.println("consumed bytes:"+res);
		  System.out.println("taille de value:"+bw.getLength());
		  byte[] test = bw.copyBytes();
		  for (int j =0;j<test.length;j++){
			 System.out.print(test[j]);
		  }
		  System.out.println(" ");
		}		
		
       in.close();		
       fr.close();	

	}

}
