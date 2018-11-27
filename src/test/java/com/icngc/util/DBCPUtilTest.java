package com.icngc.util;

import com.icngc.utils.DBCPUtil;
import org.junit.Test;

import java.sql.Connection;

/**
 * @author: Mr.Wang
 * @create: 2018-11-26 21:47
 */
public class DBCPUtilTest {
    @Test
    public void getConnection(){
        Connection connection = DBCPUtil.getConnection();
        System.out.println(connection);
    }
}
