//import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
//import java.util.*;
//import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MaxCancellationMapper extends
    Mapper<LongWritable, Text, Text, Text> 
{

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
		
	  String line = value.toString();	  
		  
	  String[] columns = line.split(",");
	  String Unique_Carrier = columns[8];
	  
	  
	  try
	  {
		  
		  String a= columns[16];
		  String b= columns[17];
		  
		
		  int e= Integer.parseInt(columns[21]);		  
		  
		  String f= a+","+b;	
		  
		  
		  if(e == 1)
		  {
			  //System.out.println("Source:Dest = "+f);
			  context.write(new Text(Unique_Carrier), new Text(f));
		  }  		  
		  		  
	  }
	  
	  catch(NumberFormatException e)
	  {
		  System.out.println("Hi Manish");
	  }      
  
    }
  }

