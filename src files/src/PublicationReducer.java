import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PublicationReducer extends
    Reducer<Text, Text, Text,Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  String referenceids="";
	 //Iterating through the list of reference id's and adding them with ',' separation 
	  for (Text value:values)
	  {
		  referenceids+=value+",";
	  }
	  
		//Emitting paper id,paper title as key and all the reference id's as value.	
	  context.write(key, new Text(referenceids.substring(0,referenceids.length()-1).trim()));
	  
  }
  }

