package hadoop;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream.
 * Depending on the constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR),
 * or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated
 * line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class FrameReader implements Closeable {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  // The line delimiter
  private final byte[] recordDelimiterBytes ={0,0};

  private ByteBuffer bf = ByteBuffer.allocate(8) ;  
  
  
  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public FrameReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public FrameReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public FrameReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param recordDelimiterBytes The delimiter
   */
  public FrameReader(InputStream in, byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * given buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public FrameReader(InputStream in, int bufferSize,
      byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>, and using a custom delimiter of array of
   * bytes.
   * @param in input stream
   * @param conf configuration
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public FrameReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    this.in = in;
    this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[this.bufferSize];
  }


  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  


  protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
      throws IOException {
    return in.read(buffer);
  }

  /**
   * Read a line terminated by a custom delimiter.
   */
  public int readFrame(BytesWritable frame, int maxFrameLength, int maxBytesToConsume,double startDate,double endDate )
      throws IOException {
   /* We're reading data from inputStream, but the head of the stream may be
    *  already captured in the previous buffer, so we have several cases:
    * 
    * 1. The buffer tail does not contain any character sequence which
    *    matches with the head of delimiter. We count it as a 
    *    ambiguous byte count = 0
    *    
    * 2. The buffer tail contains a X number of characters,
    *    that forms a sequence, which matches with the
    *    head of delimiter. We count ambiguous byte count = X
    *    
    *    // ***  eg: A segment of input file is as follows
    *    
    *    " record 1792: I found this bug very interesting and
    *     I have completely read about it. record 1793: This bug
    *     can be solved easily record 1794: This ." 
    *    
    *    delimiter = "record";
    *        
    *    supposing:- String at the end of buffer =
    *    "I found this bug very interesting and I have completely re"
    *    There for next buffer = "ad about it. record 179       ...."           
    *     
    *     The matching characters in the input
    *     buffer tail and delimiter head = "re" 
    *     Therefore, ambiguous byte count = 2 ****   //
    *     
    *     2.1 If the following bytes are the remaining characters of
    *         the delimiter, then we have to capture only up to the starting 
    *         position of delimiter. That means, we need not include the 
    *         ambiguous characters in str.
    *     
    *     2.2 If the following bytes are not the remaining characters of
    *         the delimiter ( as mentioned in the example ), 
    *         then we have to include the ambiguous characters in str. 
    */
	    int txtLength = 0; //tracks str.getLength(), as an optimization
	    int separatorLength = 0; //length of terminating newline
	    long bytesConsumed = 0;
	    int delPosn = 0;
	    int ambiguousByteCount=0; // To capture the ambiguous characters count

	    List<Byte> tram= new ArrayList<Byte>();
	 	
	    do {
	      int startPosn = bufferPosn; //starting from where we left off the last time
	      if (bufferPosn >= bufferLength) {
	        startPosn = bufferPosn = 0;   
	        bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
	        if (bufferLength <= 0)
	        	//on gere pas le fait d'avoir un fichier qui finit par zero
	          break; // EOF
	      }
	      for (; bufferPosn < bufferLength; ++bufferPosn) {
	          if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
	            delPosn++;
	            if (delPosn >= recordDelimiterBytes.length) {
	              bufferPosn++;
	              break;
	            }
	          } else if (delPosn != 0) {//it means the last byte is zero
	        	  //decode 
				byte nbZeros=buffer[bufferPosn];
				for (int k=0; k<nbZeros;k++)
					tram.add((byte) 0);
				
	            delPosn = 0;
	          }else{ //case that we have delPos=0
	        	  //ecrire le byte
	        	  tram.add(buffer[bufferPosn] );
	          }
	        	  
	        }	      	     
//	      int readLength = bufferPosn - startPosn;
//
//	      bytesConsumed += readLength;
//	      int appendLength = readLength - delPosn;
//	      if (appendLength > maxFrameLength - txtLength) {
//	        appendLength = maxFrameLength - txtLength;
//	      }
//	      if (appendLength > 0) {
//	    	
//	    	frame.set(FrameReader.transformerByte(tram),0,tram.size());// LA faire qu'ˆ la fin
//	        txtLength += appendLength;
//	      }
	      if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
	        ambiguousByteCount = delPosn;
	        bytesConsumed -= ambiguousByteCount; //to be consumed in next
	      }
	    } while (delPosn < recordDelimiterBytes.length 
	            && bytesConsumed < maxBytesToConsume);
	    
	   // we have three cases depending on the Date of frame
       bf.clear();
	   bf.put(OldFrameReader.transformerByte(tram.subList(0, 7)));
	   bf.flip();
	   double frameDate=bf.getDouble();
	   if (frameDate>endDate){ // we skip the file
		   return 0;
	   }else if (frameDate<startDate)// we skip the frame 
		   return readFrame(frame, maxFrameLength, maxBytesToConsume, startDate, endDate);
	   else { // we read the frame
		   frame.set(OldFrameReader.transformerByte(tram),0,tram.size());
	   }
	        if (bytesConsumed > (long) Integer.MAX_VALUE) {
	          throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
	        }
	        return (int) bytesConsumed; 
	      }


  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readFrame(BytesWritable frame, int maxLineLength, double startDate, double endDate) throws IOException {
    return readFrame(frame, maxLineLength, Integer.MAX_VALUE, startDate, endDate);
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(BytesWritable frame, double startDate, double endDate) throws IOException {
    return readFrame(frame, Integer.MAX_VALUE, Integer.MAX_VALUE, startDate, endDate);
  }
}
