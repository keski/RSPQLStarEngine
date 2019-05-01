package se.liu.ida.rspqlstar.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtil {
    private static Logger logger = Logger.getLogger(TimeUtil.class);
    public static long offset = 0;
    public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    static {
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static Date getTime(){
        final Date date;
        if(offset == 0){
            date = new Date();
        } else {
            date = new Date(new Date().getTime() - offset);
        }
        return date;
    }

    /**
     * Set the time offset. The offset is calculated as the unix time difference between
     * now and the reference time.
     * @param offset
     * @return
     */
    public static void setOffset(long offset){
        TimeUtil.offset = offset;
    }

    public static void silentSleep(long sleep){
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
    }
}
