package com.load.data.impala;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class ImpalaTableOp {
    final private Logger logger = LoggerFactory.getLogger(ImpalaTableOp.class);
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
    public void createTable(ArrayList<String> sqls,ArrayList<String> tablelist) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            for (int i =0;i<sqls.size();i++) {
                boolean result = stmt.execute(sqls.get(i));
                String tablename = tablelist.get(i);
                if (result) {
                    logger.error("connect impala jdbc failed");
                    break;
                }
                conn.setAutoCommit(false);
                logger.info("create table ["+tablename+"] succeed");
                System.out.println(df.format(new Date())+" create table ["+tablename+"] succeed\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("connect impala jdbc error " + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    logger.info("close stmt!");
                }
                if (conn != null) {
                    conn.close();
                    logger.info("conn stmt!");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("close conn failed!");
            }
        }
    }

    @SuppressWarnings("ReturnInsideFinallyBlock")
    public void dropTable(ArrayList<String> tablenames) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            for (String droptablename : tablenames) {
                String sql = String.format("drop table %s", droptablename);
                boolean result = stmt.execute(sql);
                if (!result) {
                    logger.info("drop impala  table[" + droptablename + "] succeed");
                    System.out.println(df.format(new Date()) + " drop impala  table[" + droptablename + "] succeed\n");
                } else {
                    logger.error("drop impala  table[" + droptablename + "] faild");
                    System.out.println(df.format(new Date()) + " drop impala  table[" + droptablename + "] faild\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("drop impala ext table error " + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    logger.info("close stmt!");
                }
                if (conn != null) {
                    conn.close();
                    logger.info("conn stmt!");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("close conn failed!");
            } finally {
                logger.info("close drop ext connect succeed!");
            }
        }
    }

    public ArrayList<String> impalaExistTable(){
        String sql = "show tables";
        Connection conn = null;
        Statement stmt = null;
        ResultSet result ;
        ArrayList<String> impalatables = new ArrayList<String>();
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            result = stmt.executeQuery(sql);
            while (result.next()) {
                String tablename = result.getString("name");
                impalatables.add(tablename);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return  impalatables;
    }
}
