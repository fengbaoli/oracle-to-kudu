package com.export.data;

import com.load.data.hdfs.HdfsOp;
import com.load.data.impala.ImpalaTableOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public  class BatchExport extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(BatchExport.class);
    private  TableToCSV tablecsv = new TableToCSV();
    private ConvertCreateTableSql convertsql = new ConvertCreateTableSql();
    private ImpalaTableOp iitc = new ImpalaTableOp();
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
    private List<String> tabList;
    private HdfsOp dfsop = new HdfsOp();
    public  BatchExport(List<String> tabList)
    {
        this.tabList = tabList;
    }
    @Override
    public void run(){
        try {
            tablecsv.startTableToCSV(tabList);
                    /*
        上传csv文件到hdfs
        */
            ArrayList<String> linux_local_filenames = new ArrayList<String>();
            ArrayList<String> hdfs_filenames = new ArrayList<String>();
            ArrayList<String> tablenames = new ArrayList<String>();
            ArrayList<String> exttables = new ArrayList<String>();
            ArrayList<String> createextsql;
            ArrayList<String> createkudusql;
            for (Object aTabList : tabList) {
                //创建hdfs存储csv文件目录
                String tablename = aTabList.toString().toLowerCase();
                tablenames.add(tablename);
                exttables.add("ext" + tablename);
                String win_local_filename = TableToCSV.local_path + "\\" + aTabList.toString() + ".csv";
                String linux_local_filename = TableToCSV.local_path + "/" + aTabList.toString() + ".csv";
                linux_local_filenames.add(linux_local_filename);
                String hdfs_filename = HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename + "/" + aTabList.toString() + ".csv";
                hdfs_filenames.add(hdfs_filename);
            }
            //上传
            try {
                //windowns
                //dfsop.uploadFile(win_local_filename,hdfs_filename);
                //linux
                 dfsop.uploadFile(linux_local_filenames, hdfs_filenames);
            } catch (Exception e) {
                e.printStackTrace();
            }
            //创建外表
            createextsql = convertsql.genCreateExtTableSql(tablenames, HdfsOp.HDFS_UPLOAD_PATH);
            iitc.createTable(createextsql,exttables);
            //System.out.println(df.format(new Date()) + " create all ext tables succeed\n");
            //创建kudu表及insert 数据
            createkudusql = convertsql.genCreateKuduTableSql(DBConnections.USERNAME, tablenames);
            iitc.createTable(createkudusql,tablenames);
            //System.out.println(df.format(new Date()) + " create all kudu tables succeed\n");
            //删除外表
            iitc.dropTable(exttables);
            //删除csv文件
             dfsop.deleteFile(hdfs_filenames);
        }catch (Exception e){
            logger.error("export data to csv error "+e.getMessage());
        }

    }
}