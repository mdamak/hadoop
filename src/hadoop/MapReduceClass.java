package hadoop;




import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceClass {

	//TODO initialize  paramList and filePath
	public static List<Integer> paramList;
	public static String filePath;
	
  public static class ExtracterMapper 
       extends Mapper<NullWritable, BytesWritable, IntWritable,CoupleWritable>{
    
    private double frameDate;
    
      
    public void map(NullWritable key, BytesWritable value, Context context
                    ) throws IOException, InterruptedException {
     ByteBuffer buffer= ByteBuffer.allocate(value.getLength());
     buffer.put(value.getBytes());
     buffer.flip();
     frameDate= buffer.getDouble();
     buffer.getInt();//size of the list of parameters
     int type = buffer.getInt();
	 int nbParam = buffer.getInt();

	   switch(type){

	   case 1 :  lireParamsVal(buffer, nbParam, context) ; break;

	   case 2 :  lireParamsVal(buffer, nbParam, context) ; break;

	   case 3 :  lireParamsStat(buffer, nbParam, context) ; break;

	   case 4 :  lireParamsMessage(buffer, nbParam, context) ; break;

	   }
    }
	   


	public  void lireParamsVal(ByteBuffer buffer,int nbParams, Context context) throws IOException, InterruptedException {
		    for (int i=0;i<nbParams;i++){
			  int id = buffer.getInt();
			  if (paramList.contains(id)){
				 double val = buffer.getDouble();
				 System.out.println(id+": "+val);
				 CoupleWritable value= new CoupleWritable(frameDate, val);
				 context.write(new IntWritable(id), value);
			  }
		    }
		}
	
	
	private void lireParamsMessage(ByteBuffer buffer, int nbParam,
			Context context) {
		 for (int i=0;i<nbParam;i++){
	    	 int id = buffer.getInt();
	    	 int taille= buffer.getInt();
	   	  if (paramList.contains(id)){
	   		String msg="";
	   		for(int j=0;j<taille; j++)
	   			msg=msg+buffer.getChar();
	   		
	   		//TODO write the value to the context
	   		 System.out.println(id + ": " + msg);
	   	  }
		 }
		 

			
		}

		private void lireParamsStat(ByteBuffer buffer, int nbParam,
				Context context) {
		    for (int i=0;i<nbParam;i++){
			  int id =buffer.getInt();
			  if (paramList.contains(id)){
				 Double tfin = buffer.getDouble();
				 Double tmin = buffer.getDouble();
				 Double tmax = buffer.getDouble();
				 Double vmin = buffer.getDouble();
				 Double vmax = buffer.getDouble();
				 Double vmoy = buffer.getDouble();
				 Double sigma = buffer.getDouble();   
				 System.out.println(id+": "+tfin+" "+tmin+" "+tmax+" "+vmin+" "+vmax+" "+vmoy+" "+sigma);
				 //TODO write to the context
			  }
		     

		    }
			
		}


  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 5) {
      System.err.println("Usage: wordcount <in> <out> <beginDate> <endDate> <paramsList>");
      System.exit(2);
    }
    
    double endDate =Double.parseDouble(args[2]);
    conf.setDouble("isa.endDate", endDate);
    double beginDate =Double.parseDouble(args[3]);
    conf.setDouble("isa.beginDate", beginDate);
   
    
    Job job = new Job(conf,"isa");
    job.setJarByClass(MapReduceClass.class);
    job.setMapperClass(ExtracterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setInputFormatClass(FramesInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
