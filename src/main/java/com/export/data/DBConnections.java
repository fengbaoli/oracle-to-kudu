package com.export.data;


import com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnections {
    private static final Logger logger = LoggerFactory.getLogger(DBConnections.class);
    private static EDKProperties pro = new EDKProperties();
    public static String URL = EDKProperties.loadProperties("ora_url");
    public static String USERNAME = EDKProperties.loadProperties("ora_username");
    public static String PASSWORD = EDKProperties.loadProperties("ora_password");
    private String url;
    private String username;
    private String password;

    public DBConnections(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection getConn()  {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("jdbc:" + url + "connect failed " + e.getMessage());
        } finally {
            logger.info("connect oracle:" + url + " succeed");
        }
        return conn;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}