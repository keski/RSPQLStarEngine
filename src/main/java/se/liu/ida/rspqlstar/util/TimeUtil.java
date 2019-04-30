package se.liu.ida.rspqlstar.util;

import java.util.Date;

public class TimeUtil {
    public static long offset = 0;

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
            // do nothing
        }
    }
}
