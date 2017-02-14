import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PublicationDriver extends Configured implements Tool {
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new PublicationDriver(),args);
    System.exit(exitCode);
  }

@Override
public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.err.println("Usage: PublicationDriver <input path> <output path>");
	      System.exit(-1);
	    }

//Initializing the map reduce job
	Job job= new Job(getConf());
	job.setJarByClass(PublicationDriver.class);
	job.setJobName("PublicationDriver");
	//Setting Input format class as DBLPInputFormat class.
	job.setInputFormatClass(DBLPInputFormat.class);

	//Setting the input and output paths.The output file should not already exist. 
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	
	
	//Setting the mapper, reducer, and combiner classes
	job.setMapperClass(PublicationMapper.class);
	job.setReducerClass(PublicationReducer.class);
	

	//Setting the format of the key-value pair to write in the output file.
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	//Submit the job and wait for its completion
	return(job.waitForCompletion(true) ? 0 : 1);
}
}
