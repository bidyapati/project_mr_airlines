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


public class AirportCount {

	
	// inner mapper class  -  to collect all airport of a country
	public static class MyMapper extends Mapper<LongWritable,Text, Text, LongWritable> {

		//mapper task for airport data file
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {	    	  
            try{
               String[] str = value.toString().split(Utils.COMMA_SEP);	 
               String country = str[Utils.AIRPORT_COUNTRY];
               context.write(new Text(country),new LongWritable(1));
            }
            catch(Exception e)
            {
               System.out.println(e.getMessage());
            }
         }  
    }
	
	//reducer to add all airports per country
	public static class ReduceSum  extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		
	    public void reduce(Text country, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		   long count = 0;
	       for (LongWritable val : values) {   
	    	   count = count + val.get();  
	       }
	       
	       context.write(country, new LongWritable(count));
		}
	}
	
	
	// command: hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.AirportCount airline/airports_mod.dat airline/airport_count_out
    public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

			Configuration conf = new Configuration();
			conf.set("mapreduce.output.textoutputformat.separator", ",");
			Job job = Job.getInstance(conf);
		    job.setJarByClass(AirportCount.class);
		    job.setJobName("Arport Count");
		    job.setMapperClass(MyMapper.class);
		    job.setNumReduceTasks(1);
		    job.setReducerClass(ReduceSum.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		  
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		    job.waitForCompletion(true);
		  
		}
	


}
