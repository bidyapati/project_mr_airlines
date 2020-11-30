# project_mr_airlines
This project is created for learning purpose only.

Pre-requisite:
1. Create a directory in HDFS
  >> hadoop fs -mkdir airline
2. Copy the airline project files into the created folder in HDFS (download the zip file and extract)
  >> hadoop fs -put airline/airports_mod.dat airline
  >> hadoop fs -put airline/Final_airlines airline
  >> hadoop fs -put airline/routes.dat airline

A. Find list of Airports operating in the Country India
Note:country name is last parameter, if not passed, defaults to India
>> hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.AirportList airline/airports_mod.dat airline/india_airport_out India

B. Find the list of Airlines having zero stops
>> hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.NonStopAirlines airline/routes.dat airline/Final_airlines airline/airlines_nonstop_out

C. List of Airlines operating with code share
>> hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.CodeShareAirlines airline/routes.dat airline/Final_airlines airline/airlines_codeshare_out

D. Which country (or) territory having highest Airports
>> hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.AirportCount airline/airports_mod.dat airline/airport_count_out

E. Find the list of Active Airlines in United state
>> hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.airline.ActiveAirlines airline/Final_airlines airline/airlines_active_out "United State"
