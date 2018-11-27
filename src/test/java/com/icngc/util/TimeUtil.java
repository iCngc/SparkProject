package com.icngc.util;

import com.icngc.utils.DateUtils;
import org.junit.Test;

/**
 * @author: Mr.Wang
 * @create: 2018-11-26 20:27
 */
public class TimeUtil {

    @Test
    public void getTodayDate(){
        String todayDate = DateUtils.getTodayDate();
        System.out.println(todayDate);
    }
}
