package cn.odyssey.marketing.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class CrossTimeQueryUtil {
    /**
     * 返回分界点
     */
    public static long getSegmentPoint(long timeStamp) {
        // 取顶-2h，等同于取底-1h
        Date ceiling = DateUtils.ceiling(new Date(timeStamp - 2 * 60 * 60 * 1000), Calendar.HOUR);
//        System.out.println(ceiling);
//        System.out.println(ceiling.getTime());
        return ceiling.getTime();
    }

    public static void main(String[] args) {
        long l = System.currentTimeMillis();
        long segmentPoint = getSegmentPoint(l);
    }
}
