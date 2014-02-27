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

	public static List<Integer> paramList;
	
  public static class ExtracterMapper 
       extends Mapper<NullWritable, BytesWritable, IntWritable,CoupleWritable>{
    
    private double frameDate;
    
      
    public void map(NullWritable key, BytesWritable value, Context context
                    ) throws IOException, InterruptedException {
     byte[] tab = value.copyBytes(); 
     ByteBuffer buffer= ByteBuffer.allocate(tab.length);
     buffer.put(tab);	
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
				 Double val = buffer.getDouble();
				 System.out.println(id+": "+val);
				 CoupleWritable value= new CoupleWritable(frameDate, val.toString());
				 context.write(new IntWritable(id), value);
			  }
		    }
		}
	
	
	private void lireParamsMessage(ByteBuffer buffer, int nbParam,
			Context context) throws IOException, InterruptedException {
		
		 for (int i=0;i<nbParam;i++){
	    	 int id = buffer.getInt();
	    	 int taille= buffer.getInt();
	   	  if (paramList.contains(id)){
	   		String msg="";
	   		for(int j=0;j<taille; j++)
	   			msg=msg+buffer.getChar();
	   		
	   		CoupleWritable value= new CoupleWritable(frameDate, msg);
			context.write(new IntWritable(id), value);
 
			System.out.println(id + ": " + msg);
	   	  }
		 }
		}
		private void lireParamsStat(ByteBuffer buffer, int nbParam,
				Context context) throws IOException, InterruptedException {
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
				String val="vmin: "+vmin +" vmax: "+vmax+" vmoy: "+vmoy;
			   	CoupleWritable value= new CoupleWritable(frameDate, val);
				context.write(new IntWritable(id), value);
			  }
		    }
		}
  }
  
  public static class CollectIdReducer 
       extends Reducer<IntWritable,CoupleWritable,IntWritable, Iterable<CoupleWritable>> {
    public void reduce(IntWritable key, Iterable<CoupleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      context.write(key, values);
    }
  }

  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 5) {
      System.err.println("Usage: wordcount <in> <out> <beginDate> <endDate> <paramsList>");
      System.exit(2);
    }
    
    double endDate =Double.parseDouble(otherArgs[2]);
    conf.setDouble("isa.endDate", endDate);
    double beginDate =Double.parseDouble(otherArgs[3]);
    conf.setDouble("isa.beginDate", beginDate);
    
    for (int i=0; i<otherArgs.length; i++)
    	paramList.add(Integer.parseInt(otherArgs[i]));
    
    Job job = new Job(conf,"isa");
    job.setJarByClass(MapReduceClass.class);
    job.setMapperClass(ExtracterMapper.class);
    job.setReducerClass(CollectIdReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setInputFormatClass(FramesInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
