package com.edureka.project.airline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver code to find airlines with any code share as Y using map-reduce
 * @author bidyapati pradhan
 *
 */
public class CodeShareAirlines {

	
	// inner mapper class  -  to collect code share per airline id
	public static class MyMapper extends Mapper<LongWritable,Text, LongWritable, Text> {

		private LongWritable outputKey = new LongWritable();
		private Text outputValue = new Text();


		//mapper task for route file
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	// reading each route and check airline id and code share values
        	String row = value.toString();
        	String[] tokens = row.split(Utils.COMMA_SEP);
        	try {
	        	Long id = Long.parseLong(tokens[Utils.ROUTE_AL_ID]);
	        	String codeShare = tokens[Utils.ROUTE_CODE_SHARE];
	        	outputKey.set(id);
	        	outputValue.set(codeShare);
	      	  	context.write(outputKey,outputValue);
        	} catch (Exception ee) {
        		// ignore the invalid rows
        		return;
        	}
        }  
    }
	
	//reducer to check all code share are Y or not
	public static class ReduceSum  extends Reducer<LongWritable,Text,LongWritable,Text>
	{
		// a map to store airline id to name
		private Map<Long, String> mapAirlineID2Name = new HashMap<Long, String>();
		
		
		// reducer side initialization
		//init setup method to read and store mapping between airline id and name
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);
		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    // get the airline file from cache
		    Path finalAirLinesFile = new Path(files[0]);
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
		    // if it is airline file, fill the id to name map
			if (finalAirLinesFile.getName().equalsIgnoreCase(Utils.FILE_AIRLINES)) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(finalAirLinesFile)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(Utils.COMMA_SEP);
						Long id = Long.parseLong(tokens[Utils.AIRLINE_ID]);
						String name = tokens[Utils.AIRLINE_NAME];
						mapAirlineID2Name.put(id, name);
						line = reader.readLine();
					}
					reader.close();
			}

		
			
			if (mapAirlineID2Name.isEmpty()) {
				throw new IOException("MyError:Unable to load airline data.");
			}

		}
		
	    public void reduce(LongWritable airlineId, Iterable<Text> csValues,Context context) throws IOException, InterruptedException {
		   Boolean codeShareFlag = false;

		   //if any of the code share is Y for an airline
	       for (Text val : csValues) {   
	    	   String codeShare = val.toString();  
	    	   if (codeShare.equalsIgnoreCase("Y")) {
	    		   codeShareFlag = true;
	    		   break;
	    	   }
	       }
	       
	       if (codeShareFlag && (null != mapAirlineID2Name.get(airlineId.get()))) {
	           Text result = new Text();
		       result.set(mapAirlineID2Name.get(airlineId.get()));		      
		       context.write(airlineId, result);
	       }

		}
	}
	
	
	// command: hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.CodeShareAirlines airline/routes.dat airline/Final_airlines airline/airlines_nonstop_out
        public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformat.separator", ",");
		    Job job = Job.getInstance(conf);
		    job.setJarByClass(NonStopAirlines.class);
		    job.setJobName("Airlines with Code Share");
		    job.setMapperClass(MyMapper.class);
		    job.addCacheFile(new Path(args[1]).toUri());
		    job.setNumReduceTasks(1);
		    job.setReducerClass(ReduceSum.class);
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		  
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
		    job.waitForCompletion(true);
		  
		  
		}
	

}
