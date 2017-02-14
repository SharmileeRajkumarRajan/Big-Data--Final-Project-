import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
public class PublicationMapper extends
    Mapper<LongWritable, Text,Text,Text> {
  public void map(LongWritable key,Text value, Context context)
      throws IOException, InterruptedException {
	  
	  //Extracting the fields which came from DBLPInput format class.
	  String record[] = value.toString().split("\n");
	  String paperID=null,referenceID=null,paperTitle=null;
	 
	  //Extracting the paper title and paper id
	  for(int i=0;i<record.length;i++)
	  {
		  if(record[i].startsWith("#index"))
		  	 paperID=record[i].substring(6, record[i].length()).trim();
		  		
		  if(record[i].startsWith("#*"))
			  paperTitle=record[i].substring(2,record[i].length()).trim();
		  
	  }
	  //Extracting the reference id's and emitting every reference id as value
	  for(int i=0;i<record.length;i++)
	  {
		   if(record[i].startsWith("#%") && record[i].length()>2)
		  {
			  referenceID=record[i].substring(2, record[i].length()).trim();
		  }
		   //checking for null paper id  and reference id
		  if(paperID!=null && !paperID.isEmpty() && referenceID!=null &&!referenceID.isEmpty() && !record[i].startsWith("#!") && paperTitle!=null)
		  {
			  //Emitting paper id and paper title with tab separated as key and reference id as value.
			  context.write(new Text(paperID+"	"+paperTitle), new Text(referenceID));
			  
		  }
		  
	  }
	  
  }
 }

