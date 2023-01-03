/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.common.utils;

import io.dingodb.dailytest.CommonArgs;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作数据库的工具类
 */

public class JDBCUtils {
    static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    static final String USER = "xxx";
    static final String PASS = "xxx";

    //获取数据库连接
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//        String defaultConnectIP = "172.20.3.27";
        String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
//        String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
        String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765/db?timeout=36000";

        //加载驱动
        Class.forName(JDBC_DRIVER);

        //获取连接
        Connection conn = DriverManager.getConnection(connectUrl, USER, PASS);
        return conn;
    }

    //获取所有数据表
    public static List<String> getTableList() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet resultSetSchema = dmd.getSchemas();
        List<String> schemaList = new ArrayList<>();
        while (resultSetSchema.next()) {
            schemaList.add(resultSetSchema.getString(1));
        }

        List<String> tableList = new ArrayList<String>();
//        ResultSet rst = dmd.getTables(null, schemaList.get(0), "%", null);
        ResultSet rst = dmd.getTables(null, "DINGO", "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toUpperCase());
        }
        rst.close();
        resultSetSchema.close();
        return tableList;
    }


    //关闭资源: 连接和statement
    public static void closeResource(Connection conn, Statement ps) {
        try{
            if(ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //关闭资源: resultSet, 连接和statement
    public static void closeResource(ResultSet rs, Connection conn, Statement ps) {
        try{
            if(rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
