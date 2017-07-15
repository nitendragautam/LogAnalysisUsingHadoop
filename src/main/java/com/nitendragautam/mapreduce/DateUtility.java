package com.nitendragautam.mapreduce;


import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtility {

    public static String getTodaysDate(){
       SimpleDateFormat dateFormat =
               new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,0);
        return dateFormat.format(cal.getTime());

    }
}
