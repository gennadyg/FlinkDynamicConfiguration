package com.dataartisans.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Gennady.Gilin on 6/23/2016.
 */
public class TimeUtils {

    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss:SSS");

    public static String timestampToString( long timeStamp ){

        Instant fromUnixTimestamp = Instant.ofEpochSecond( timeStamp );
        LocalDateTime localDateTime = LocalDateTime.ofInstant( fromUnixTimestamp, ZoneId.systemDefault() );
        String timeStampString = localDateTime.format( format );
        //String timeStampString = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format( new Date( timeStamp ));
        return timeStampString;
    }

    public static String currentDataToString(){

        String timeStampString = LocalDateTime.now().format( format );
        //String timeStampString = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format( new Date( timeStamp ));
        return timeStampString;
    }

    public static String currentDateToCustomFormat( String format ){

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern( format );
        String timeStampString = LocalDateTime.now().format( dateTimeFormatter );
        //String timeStampString = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format( new Date( timeStamp ));
        return timeStampString;
    }

    public static void main( String[] args ){

        String value = "Google,1,yyyy-MM-dd hh:mm:ss:SSS";

        String[] splits = value.split(",");
        System.out.println( splits.length );

        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        Matcher matcher = pattern.matcher( value );

        while (matcher.find())
            System.out.println(matcher.group(1));
        }


}
