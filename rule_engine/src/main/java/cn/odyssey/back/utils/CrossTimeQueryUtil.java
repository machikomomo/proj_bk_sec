package cn.odyssey.back.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class CrossTimeQueryUtil {
    /**
     * 写一个static方法 传入时间戳 返回分界点时间戳
     */
    public static long getSegmentPoint(long timeStamp) {
        Date ceiling = DateUtils.ceiling(new Date(timeStamp - 2 * 60 * 60 * 1000), Calendar.HOUR);
//        System.out.println(ceiling);
        return ceiling.getTime();
    }

    public static void main(String[] args) {
        getSegmentPoint(System.currentTimeMillis());
    }
}
