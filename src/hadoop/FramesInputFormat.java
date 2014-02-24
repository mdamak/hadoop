package hadoop;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class FramesInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	  @Override
	  public RecordReader<NullWritable,BytesWritable> createRecordReader(InputSplit split,
	                       TaskAttemptContext context) {
	    return new FrameRecordReader();
	  }

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }


}
