package com.util;

import com.export.data.BatchExport;
import com.export.data.DBConnections;
import com.load.data.hdfs.HdfsOp;
import com.load.data.impala.ImpalaTableOp;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class LoadDataMain {
    private static final Logger logger = LoggerFactory.getLogger(LoadDataMain.class);

    //配置log4j位置
    static {
        PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf" + File.separator
                + "log4j.properties");
    }
    //导出用户表数据到csv文件

    public static void main(String args[]) throws IOException {
        Connection conn=null;
        Statement stmt=null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String batch_size = EDKProperties.loadProperties("batch_size");
        HdfsOp dfsop = new HdfsOp();
        IsTableSkip ifskiptab = new IsTableSkip();
        String sql;
        sql = "select table_name from user_tables";
        List<String> tabList = new ArrayList<String>();
        IsTablePk ispk = new IsTablePk();
        List<List<String>> bachTabList = new ArrayList<List<String>>();

        /*
         创建hdfs上传路径
        */
        try {
            dfsop.makeDir(HdfsOp.HDFS_UPLOAD_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                String table = rs.getString(1);
                //判断表是否存在主键
                if (ispk.isContainPk(table, DBConnections.USERNAME)) {
                    //判断表是否需要同步
                    if (!ifskiptab.ifskipTable(table.toLowerCase())) {
                        tabList.add(table);
                    }
                }
            }
            stmt.close();
            conn.close();
            //删除impala存在的表
            ImpalaTableOp impalaop = new ImpalaTableOp();
            ArrayList<String> droptables = impalaop.impalaExistTable();
            if(droptables.size()>0){
                impalaop.dropTable(droptables);
            }

            //表生成csv文件,使用多线程
            //table 分批处理
            if (tabList.size() > 0) {
                assert batch_size != null;
                int pointsDataLimit = Integer.parseInt(batch_size);
                Integer size = tabList.size();
                //判断是否有必要分批
                if (pointsDataLimit < size) {
                    int part = (size - 1) / pointsDataLimit + 1;//分批数
                    int startNum = 0;
                    for (int i = 0; i < part; i++) {
                        int endNum = startNum + pointsDataLimit;
                        if (endNum >= size) {
                            endNum = size;
                        }
                        List<String> listPage = tabList.subList(startNum, endNum);
                        bachTabList.add(listPage);
                        startNum += pointsDataLimit;
                    }
                } else {
                    //不够分批表运行
                    bachTabList.add(tabList);
                }
            } else {
                System.out.println("no table need to export");
            }
            //多线程导出并创建表
            ExecutorService exe = Executors.newFixedThreadPool(bachTabList.size());
            for (List<String> aBachTabList : bachTabList) {
                exe.execute(new BatchExport(aBachTabList));
            }
            exe.shutdown();
            while (true){
                if (exe.isTerminated()){
                    System.out.println(df.format(new Date()) + " Load table "+tabList+" data to kudu succeed\n");
                    logger.info(" Load table "+tabList+" data to kudu succeed");
                    break;
                }
            }
            } catch(Exception e){
            e.printStackTrace();
        } finally{
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
        }
    }

