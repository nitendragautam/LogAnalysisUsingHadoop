package com.nitendragautam.project.services;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nitendragautam on 5/27/2017.
 */
public class AccessLogsParser {
/*
    Parses the Access Logs by Passing Single line
    Input is the single line of Log Record in combined Log Format
    output is the Access Record Object
     */

    public AccessLogRecord parseAccessLogs(String logRecord){
        AccessLogRecord accessLogRecord =null;
        Pattern pattern = getLogsPattern(); //Gets the Access Logs Pattern
        Matcher regexMatcher =pattern.matcher(logRecord);
        if(regexMatcher.find()==true){ //If Pattern is Macthed
            accessLogRecord = buildAccessLogRecord(regexMatcher);
        }else{ //Return Null Access Log Record

            accessLogRecord=new AccessLogRecord("","","","","","","","","");
        }

        return accessLogRecord;
    }




    private AccessLogRecord buildAccessLogRecord(Matcher matcher){
        return new AccessLogRecord(
                matcher.group(1),
                matcher.group(2),
                matcher.group(3),
                matcher.group(4),
                matcher.group(5),
                matcher.group(6),
                matcher.group(7),
                matcher.group(8),
                matcher.group(9)
        );

    }
    /**
     passed Paramater :Http Request Field like "GET /history/skylab/skylab-small.gif HTTP/1.0"
     *returns a Tuple of (requestType ,uri ,httpVersion)
     */
    public Triple parseHttpRequestField(String httpRequest){
        Triple triple;
        String splittedArray[] =httpRequest.split(" ");//Split the Request based on Empty Space
        if(splittedArray.length ==3){
            triple = new ImmutableTriple(splittedArray[0],splittedArray[1],splittedArray[2]);
        }else{
            triple = new ImmutableTriple("","","");
        }
        return triple;
    }

    /*
    Parses the Date Field eg : "[21/Jun/2010:02:48:13 -0700]"
    and gives the epoch time
     */
    public String  parseDateField(String dateField){
        String dateFormat = "([\\w:/]+\\s[+\\-]\\d{4})";
        String parsedDate=null;
        Pattern datePattern = Pattern.compile(dateFormat); //Using
        Matcher dateMatcher = datePattern.matcher(dateField);

        if(dateMatcher.find()){  //If True
            String dateString = dateMatcher.group(1); //Match the Date
            SimpleDateFormat localDateFormat =
                    new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
            try {

                long epochTime =localDateFormat.parse(dateString).getTime();
                SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:MM:SS");
                parsedDate=sdf.format(new Date(epochTime));



            }catch(ParseException e){
                e.printStackTrace();;

            }
        }else{
            parsedDate=null;
        }
        return parsedDate;
    }



    private Pattern getLogsPattern(){

        //At least 1 but not more than 3 times eg:192 or 92
        String clientIpAddress =  "^([\\d.]+) ";  //Eg 192.168.138.1
        String clientIdentity = "(\\S+) ";   //  '\S' is non whitespace character
        String remoteUser  = "(\\S+) ";     //Any non white Space Character
        String dateTime = "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] ";     //Eg :`[21/Jun/2010:08:45:13 -0700]`
        String httpRequest = "\"(.+?)\" ";     //any number of any characters
        String httpStatus = "(\\d{3}) ";
        String requestBytes = "(\\d+) ";     //Can be empty or '-'
        String siteReferer = "\"([^\"]+)\" ";  //Site Referer
        String userAgent  =  "\"([^\"]+)\"";    //User Agents
        String accesLogRegex = clientIpAddress+clientIdentity+remoteUser+dateTime+httpRequest+httpStatus+requestBytes+siteReferer+userAgent;
        Pattern pattern = Pattern.compile(accesLogRegex);
        return pattern;
    }
}