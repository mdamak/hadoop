package hadoop;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class FrameRecordReader extends RecordReader<NullWritable, BytesWritable> {
 
  public static final String MAX_LINE_LENGTH = 
		    "mapreduce.input.linerecordreader.line.maxlength";
  

  private long start;
  private long pos;
  private long end;
  private FrameReader in;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  private int maxLineLength;
  private NullWritable key = null;
  private BytesWritable value = null;
  private double endDate;
  private double startDate;
  private byte[] recordDelimiterBytes={0,0};

  public FrameRecordReader() {
  }

  public FrameRecordReader(byte[] recordDelimiter) {
	    this.recordDelimiterBytes = recordDelimiter;
	  }
  
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    
    this.endDate= job.getDouble("isa.endDate", 0);
    this.startDate= job.getDouble("isa.startDate", 0);

    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    
    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);

    fileIn.seek(start);
    in = new FrameReader(fileIn, job, recordDelimiterBytes);
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
    start += in.readFrame(new BytesWritable(),maxLineLength, startDate, endDate);
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

  
  public boolean nextKeyValue() throws IOException {
	
    if (value == null) {
      value = new BytesWritable();
    }
    int newSize = 0;
	while (pos <= end ) {
      newSize = in.readFrame(value, maxLineLength,startDate,endDate);
                            
      
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

   
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
