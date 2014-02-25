package hadoop;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class testHadoop {

	public static void main(String[] args) throws IOException {

     	ByteBuffer bf = ByteBuffer.allocate(7);
		//resultqt attendu : 1|2|0|0|0|4
		bf.put((byte) 1);bf.put((byte) 2);bf.put((byte) 0);bf.put((byte) 3);bf.put((byte) 4);bf.putShort((byte) 0);
		byte[] trame = bf.array();
		FileOutputStream fos = new FileOutputStream("/Users/Hamza/Desktop/trameTest.bin");
		fos.write(trame);
		fos.close();
       
		
		
						
	}

}
