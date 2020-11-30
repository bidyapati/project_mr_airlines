package com.edureka.project.airline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * Find the active Airlines in US
 * Pre-requisite - load the airport related files into airline directory in HDFS
 * command: hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.ActiveAirlines airline/Final_airlines airline/active_airlines_US_out
 * @author bidyapati pradhan
 *
 */

public class ActiveAirlines {
	
	// inner mapper class  -  to collect active airlines
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		//mapper task for airline data file
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	String row = value.toString();
        	String[] tokens = row.split(Utils.COMMA_SEP);
        	try {
	        	String active = tokens[Utils.AIRLINE_ACTIVE];
	        	String country = tokens[Utils.AIRLINE_COUNTRY];
	        	String name = tokens[Utils.AIRLINE_NAME];
	        	outputKey.set(country);
	        	outputValue.set(name);
	        	// write into mapper output if it is active airline
	        	if (active.equalsIgnoreCase("Y")) {
	      	  	    context.write(outputKey,outputValue);
	        	}
        	} catch (Exception ee) {
        		// ignore the invalid rows
        		return;
        	}
        }  
    }
	
	//reducer to filter country name
	public static class ReduceSum  extends Reducer<Text,Text,Text,Text>
	{
		
	    public void reduce(Text country, Iterable<Text> airlines,Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        String countryFilter = conf.get(Utils.FILTER_COUNTRY).toLowerCase();
	        String countryLC = country.toString().toLowerCase();
	        //if filter country does not starts with this country name, skip writing
	        if (countryLC.indexOf(countryFilter) == -1) {
	        	return;
	        }
	    	
	        StringBuilder sbAirlines = new StringBuilder();
	        for (Text val : airlines) {   
	        	sbAirlines.append(val.toString());
	        	sbAirlines.append(",");
	        }
	       
	        context.write(country, new Text(sbAirlines.toString()));

		}
	}
	
	
	// command: hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.ActiveAirlines airline/Final_airlines airline/airlines_active_out
    public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

			Configuration conf = new Configuration();
			conf.set("mapreduce.output.textoutputformat.separator", ",");

		    if(args.length == 3) {
		        conf.set(Utils.FILTER_COUNTRY, args[2]);
		    } else {
		    	//default filter to US
		    	conf.set(Utils.FILTER_COUNTRY, "United State");
		    }
			Job job = Job.getInstance(conf);
		    job.setJarByClass(ActiveAirlines.class);
		    job.setJobName("Active Airlines");
		    job.setMapperClass(MyMapper.class);
		    job.setNumReduceTasks(1);
		    job.setReducerClass(ReduceSum.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		  
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		    job.waitForCompletion(true);
		  
		  
		}
	
}
