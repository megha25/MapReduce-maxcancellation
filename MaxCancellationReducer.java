import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MaxCancellationReducer extends
    Reducer<Text, Text, Text, Text>
{
	
	
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException 
  {	 
	  int max=0;	  
	  String r="", max_key = "";
	  String p = null;
	  Integer count=0;
	  Integer q = 0;
	  
	  HashMap<String,Integer> map = new HashMap<>();
   
    for (Text str : values) 
    {    	  	
    	count = map.get(str.toString());
    	if(map.containsKey(str.toString()))
    	{
    		map.put(str.toString(), count+1);
    	}
    	else
    	{
    		map.put(str.toString(), 1);
    	}
    	//map.put(str.toString(), (count == null) ? 1 : count+1);    	        	
    }
    
    
    for(Map.Entry<String,Integer> entry : map.entrySet())
    {
    	 	
    		p = entry.getKey().toString();
    	    q = entry.getValue();
    		
    		if(max<q)
    		{
    			max=q;
    			max_key = p;
    		}  
         
    }
    r = max_key +" "+ max; 
    context.write(key, new Text(r)); 
  }
  //still I do not see anywhere in your reducer that you find the entry with the maximum count in the 
  //reducer. All I see is that you go through all the map entries and emit all of them. Instead 
  //you should try to find the entry in the map that has the maximum count and only emit that.
	     
}
