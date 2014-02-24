package hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.io.BytesWritable;

/**
 * A class that provides a frame reader from an input stream.
 */
public class OldFrameReader {

	
	
	
	  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	  private int bufferSize = DEFAULT_BUFFER_SIZE;
	  private InputStream in;
	  private byte[] buffer;
	  // the number of bytes of real data in the buffer
	  private int bufferLength = 0;
	  // the current position in the buffer
	  private int bufferPosn = 0;


	  /**
	   * Create a frame reader that reads from the given stream using the
	   * default buffer-size (64k).
	   * @param in The input stream
	   * @throws IOException
	   */
	  public OldFrameReader(InputStream in) {
	    this(in, DEFAULT_BUFFER_SIZE);
	  }

	  /**
	   * Create a frame reader that reads from the given stream using the 
	   * given buffer-size.
	   * @param in The input stream
	   * @param bufferSize Size of the read buffer
	   * @throws IOException
	   */
	  public OldFrameReader(InputStream in, int bufferSize) {
	    this.in = in;
	    this.bufferSize = bufferSize;
	    this.buffer = new byte[this.bufferSize];
	  }

	  /**
	   * Create a frame reader that reads from the given stream using the
	   * <code>io.file.buffer.size</code> specified in the given
	   * <code>Configuration</code>.
	   * @param in input stream
	   * @param conf configuration
	   * @throws IOException
	   */
	  public OldFrameReader(InputStream in, Configuration conf) throws IOException {
	    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
	  }

	  /**
	   * Close the underlying stream.
	   * @throws IOException
	   */
	  public void close() throws IOException {
	    in.close();
	  }
	  
	  /**
	   * Read one frame from the InputStream into the given frame.  A line
	   * can be terminated by one of the following: '\n' (LF) , '\r' (CR),
	   * or '\r\n' (CR+LF).  EOF also terminates an otherwise unterminated
	   * line.
	   *
	   * @param frame the object to store the given line (without newline)
	   * @param maxFrameLength the maximum number of bytes to store into str;
	   *  the rest of the line is silently discarded.
	   * @param maxBytesToConsume the maximum number of bytes to consume
	   *  in this call.  This is only a hint, because if the line cross
	   *  this threshold, we allow it to happen.  It can overshoot
	   *  potentially by as much as one buffer length.
	   *
	   * @return the number of bytes read including the (longest) newline
	   * found.
	   *
	   * @throws IOException if the underlying stream throws
	   */
	  public int readFrame(BytesWritable frame, int maxFrameLength,
	                      int maxBytesToConsume) throws IOException {
	    /* We're reading data from in, but the head of the stream may be
	     * already buffered in buffer, so we have several cases:
	     * 1. No newline characters are in the buffer, so we need to copy
	     *    everything and read another buffer from the stream.
	     * 2. An unambiguously terminated line is in buffer, so we just
	     *    copy to frame.
	     * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
	     *    in CR.  In this case we copy everything up to CR to frame, but
	     *    we also need to see what follows CR: if it's LF, then we
	     *    need consume LF as well, so next call to readLine will read
	     *    from after that.
	     * We use a flag prevCharCR to signal if previous character was CR
	     * and, if it happens to be at the end of the buffer, delay
	     * consuming it until we have a chance to look at the char that
	     * follows.
	     */
		frame.setSize(0);// clean bytes
	    frame= new BytesWritable();// TODO au lieu de str.clear() , je ne sais passi c'est bon 
	    int txtLength = 0; //tracks str.getLength(), as an optimization
	    int separatorLength = 0; //length of terminating newline
	    long bytesConsumed = 0;

	    List<Byte> tram= new ArrayList<Byte>();
	 	int cptZero=0;
	 	
	    do {
	      int startPosn = bufferPosn; //starting from where we left off the last time
	      if (bufferPosn >= bufferLength) {
	        startPosn = bufferPosn = 0;
	        
//	        if (prevCharCR)
//	          ++bytesConsumed; //account for CR from previous read
	        
	        bufferLength = in.read(buffer);
	        if (bufferLength <= 0)
	          break; // EOF
	      }
	      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
	        if (buffer[bufferPosn] == 0) {
	          cptZero++;
	          if (cptZero==3){
	        	  cptZero=0;
					 if (buffer[bufferPosn+1]==0 && buffer[bufferPosn+2]==0){ // five zeros! separator  :)
						 separatorLength=5;
					    // TODO delete zeros if i added it
						 tram.remove(tram.size()-1);tram.remove(tram.size()-1);					 
						 bufferPosn+=2; // increment buffer position 
//						 bf.position(bf.position()+2);
//						 tram.clear();
						 break; // end of frame
					 }else{			
						 // TODO add the byte to the frame 
						 tram.add(buffer[bufferPosn] );
						 byte nbZeros=buffer[bufferPosn+1];
						 for (int k=0; k<nbZeros;k++)
							 tram.add((byte) 0);
						 bufferPosn++; // increment the position 
					 }
	          }else{ // cptZero !=3
	        	// TODO add the byte to the frame
	        	  tram.add(buffer[bufferPosn] );
	          }
	        
	        }else{ // we read  1
	        	cptZero=0;
	        	// TODO add the byte to the frame 
	        	 tram.add(buffer[bufferPosn] );
	        }
	      }// end of buffer
				 
	      int readLength = bufferPosn - startPosn;

	      bytesConsumed += readLength;
	      int appendLength = readLength - separatorLength;
	      if (appendLength > maxFrameLength - txtLength) {
	        appendLength = maxFrameLength - txtLength;
	      }
	      if (appendLength > 0) {
	    	  frame.setSize(tram.size());
	    	  frame.set(OldFrameReader.transformerByte(tram),0,tram.size());
	       // frame.set(buffer, startPosn, appendLength);
	        txtLength += appendLength;
	      }
	    } while (separatorLength == 0 && bytesConsumed < maxBytesToConsume);
	    if (bytesConsumed > (long)Integer.MAX_VALUE)
	      throw new IOException("Too many bytes before newline: " + bytesConsumed);    
	    return (int)bytesConsumed;
	    
	  }

	  /**
	   * Read from the InputStream into the given Text.
	   * @param trame the object to store the given line
	   * @param maxLineLength the maximum number of bytes to store into str.
	   * @return the number of bytes read including the newline
	   * @throws IOException if the underlying stream throws
	   */
	  public int readFrame(BytesWritable trame, int maxLineLength) throws IOException {
	    return readFrame(trame, maxLineLength, Integer.MAX_VALUE);
	}

	  /**
	   * Read from the InputStream into the given Text.
	   * @param trame the object to store the given line
	   * @return the number of bytes read including the newline
	   * @throws IOException if the underlying stream throws
	   */
	  public int readFrame(BytesWritable trame) throws IOException {
	    return readFrame(trame, Integer.MAX_VALUE, Integer.MAX_VALUE);
	  }


public static byte[] transformerByte(List<Byte> tab) {

	byte[] res = new byte[tab.size()];

	for (int i=0;i<tab.size();i++){
		res[i]=tab.get(i).byteValue();
	}

	return res;
}
	}
