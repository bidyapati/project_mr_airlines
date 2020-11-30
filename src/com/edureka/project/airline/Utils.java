package com.edureka.project.airline;

public class Utils {
	// file names
	// airport data file
	public final static String FILE_AIRPORTS = "airports_mod.dat";
	// airlines data file
	public final static String FILE_AIRLINES = "Final_airlines";
	// route data file
	public final static String FILE_ROUTES = "routes.dat";
		
	//data separator
	public final static String COMMA_SEP = ",";
	//configuration to filter the country
	public final static String FILTER_COUNTRY = "filter.country.name";
	/*
	Air Ports data set i.e airports_mod.dat
	0 Airport ID
	1 Name of airport
	2 Main city served by airport
	3 Country or territory where airport is located.
	4 3-letter FAA code
	5 4-letter ICAO code
	6 Latitude
	7 Longitude
	8 Altitude
	9 Hours offset from UTC
	10 Daylight savings time
	11 time zone
	 */
    public final static Integer AIRPORT_ID = 0;//LONG
    public final static Integer AIRPORT_NAME = 1;//TEXT
    public final static Integer AIRPORT_CITY = 2;//TEXT
    public final static Integer AIRPORT_COUNTRY  = 3;//TEXT
    public final static Integer AIRPORT_FAA_CODE = 4;//TEXT
    public final static Integer AIRPORT_ICAO_CODE = 5;//TEXT
    public final static Integer AIRPORT_LATITUDE = 6;//DOUBLE
    public final static Integer AIRPORT_LONGITUDE = 7;//DOUBLE
    public final static Integer AIRPORT_ALTITUDE = 8;//LONG
    public final static Integer AIRPORT_OFFSET_HOURS = 9;//FLOAT
    public final static Integer AIRPORT_DST = 10;//TEXT
    public final static Integer AIRPORT_TIMEZONE = 11;//TEXT
    
    /**
     * Airline Data
     * 0 Airline ID
     * 1 Airline name
     * 2 Alias name
     * 3 IATA code
     * 4 ICAO code
     * 5 Airline callsign
     * 6 Country or territory where airline is incorporated
     * 7 Active Y/N
     */
    public final static Integer AIRLINE_ID = 0;//LONG
    public final static Integer AIRLINE_NAME = 1;//TEXT
    public final static Integer AIRLINE_ALIAS = 2;//TEXT
    public final static Integer AIRLINE_IATA  = 3;//TEXT
    public final static Integer AIRLINE_ICAO = 4;//TEXT
    public final static Integer AIRLINE_CALLSIGN = 5;//TEXT
    public final static Integer AIRLINE_COUNTRY = 6;//TEXT
    public final static Integer AIRLINE_ACTIVE = 7;//TEXT
    
    /**
     * Route data
     * 0 Airline code : IATA or ICAO code
     * 1 Airline ID
     * 2 Source Airport Code
     * 3 Source Airport ID
     * 4 Destination airport Code
     * 5 Destination airport ID
     * 6 Code share Y/N
     * 7 Number of stops on this flight ("0" for direct)
     * 8 Equipment
     */
    public final static Integer ROUTE_AL_CODE = 0;//TEXT
    public final static Integer ROUTE_AL_ID = 1;//LONG
    public final static Integer ROUTE_SRC_CODE = 2;//TEXT
    public final static Integer ROUTE_SRC_ID  = 3;//LONG
    public final static Integer ROUTE_DST_CODE = 4;//TEXT
    public final static Integer ROUTE_DST_ID = 5;//LONG
    public final static Integer ROUTE_CODE_SHARE = 6;//TEXT
    public final static Integer ROUTE_STOPS = 7;//LONG
    public final static Integer ROUTE_EQUIPMENT = 8;//TEXT
}
