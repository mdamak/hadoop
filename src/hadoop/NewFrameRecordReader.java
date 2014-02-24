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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class FrameRecordReader implements RecordReader<NullWritable, BytesWritable> {
  private static final Log LOG
    = LogFactory.getLog(LineRecordReader.class.getName());

  private long start;
  private long pos;
  private long end;
  private FrameReader in;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  

  public FrameRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    this(job, split, null);
  }

  public FrameRecordReader(Configuration job, FileSplit split,
      byte[] recordDelimiter) throws IOException {
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    
    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
  
      fileIn.seek(start);
      in = new FrameReader(fileIn, job, recordDelimiter);
      filePosition = fileIn;
    
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    //si ce n'est pas le premier split on positionne le buffer correctement 
    //deux cas : soit le split commence avec un zero (delimiteur possible) soit non  
    if (start != 0) {
      if (finDelimiteur()){
    	 start++; 
      }
      else{
      start += in.readFrame(new BytesWritable());
      }
     }
    this.pos = start;
       
  }

  //methode qui sert a savoir si le split commence avec la fin d'un delimiteur
  public boolean finDelimiteur() throws IOException{
    boolean res = false;
     fileIn.mark(10);
     byte[] buff = new byte[2];
     long pos = (long) (fileIn.getPos()-1);
     fileIn.read(pos, buff, 0, 2);
     if (buff[0]==0 && buff[1]==0){
    	 res=true;
     }
	 fileIn.reset();
	return res;  
  }

  
  
  
  public FrameRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this(in, offset, endOffset, maxLineLength, null);
  }

  public FrameRecordReader(InputStream in, long offset, long endOffset,
      int maxLineLength, byte[] recordDelimiter) {
    this.in = new FrameReader(in, recordDelimiter);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
    filePosition = null;
  }

  public FrameRecordReader(InputStream in, long offset, long endOffset,
                          Configuration job)
    throws IOException{
    this(in, offset, endOffset, job, null);
  }

  public FrameRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job, byte[] recordDelimiter)
    throws IOException{
    this.in = new FrameReader(in, job, recordDelimiter);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
    filePosition = null;
  }

  public NullWritable createKey() {
    return NullWritable.get();
  }
  
  public BytesWritable createValue() {
    return new BytesWritable();
  }
  

  private long getFilePosition() throws IOException {
    long retVal;
      retVal = pos;
    return retVal;
  }

  /** Read a line. */
  public synchronized boolean next(NullWritable key, BytesWritable value)
    throws IOException {
	 	  
     byte[] bytes = new  byte[bufferSize];
	 value = new BytesWritable(bytes);  
	 
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end) {  	
      int newSize = in.readLine(value.getBytes());
      if (newSize == 0) {
        return false;
      }
      pos += newSize;

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  /**
   * Get the progress within the split
   */
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
  
    }
  }
}
