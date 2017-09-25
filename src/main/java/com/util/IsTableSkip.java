package com.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017/9/20.
 */

class IsTableSkip {
    private static final Logger logger = LoggerFactory.getLogger(IsTableSkip.class);
    private EDKProperties pro = new EDKProperties();
    private String skiptables = EDKProperties.loadProperties("skip_tables");
    boolean ifskipTable(String tablename) {
        if (skiptables != null && skiptables != "") {
            boolean flag;
            List<String> list = Arrays.asList(skiptables.split(","));
            if(list.contains(tablename)){
                flag = true;
                logger.info("table ["+tablename+"] is not need sync,skip this table" );
            }else {
                flag = false;
            }
            return  flag;
        }else {
            return false;
        }
    }
}
