package com.edureka.project.airline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Program to list airport names of a country(last parameter or else India as default)
 * Pre-requisite - load the airport related files into airline directory in HDFS
 * command: hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.AirportList airline/airports_mod.dat airline/india_airport_out India
 * @author bidyapati pradhan
 *
 */

// mapper class to output key as 'airport name' and value as 'city,country'
class AirportListMapper extends Mapper<LongWritable,Text,Text,Text>
{
  public void map(LongWritable key, Text value, Context context)
  {	    	  
     Configuration conf = context.getConfiguration();
     String countryFilter = conf.get(Utils.FILTER_COUNTRY);
     try{
        String[] str = value.toString().split(Utils.COMMA_SEP);	 
        String name = str[Utils.AIRPORT_NAME];
        String city = str[Utils.AIRPORT_CITY];
        String country = str[Utils.AIRPORT_COUNTRY];
        StringBuilder cityloc = new StringBuilder();
        cityloc.append(city);
        cityloc.append(Utils.COMMA_SEP);
        cityloc.append(country);

        if (countryFilter.equalsIgnoreCase(country)) {
            context.write(new Text(name),new Text(cityloc.toString()));
        }
     }
     catch(Exception e)
     {
        System.out.println(e.getMessage());
     }
  }
}

// entry method to filter airport list
// third parameter is the country name to filter, default value is India
public class AirportList {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    if(args.length == 3) {
	        conf.set(Utils.FILTER_COUNTRY, args[2]);
	    } else {
	    	//default filter to India
	    	conf.set(Utils.FILTER_COUNTRY, "india");
	    }
	    
	    Job job = Job.getInstance(conf, "Airport List of a country");
	    job.setJarByClass(AirportList.class);
	    job.setMapperClass(AirportListMapper.class);
	    job.setNumReduceTasks(0);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
