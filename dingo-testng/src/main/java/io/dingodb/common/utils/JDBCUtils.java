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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 操作数据库的工具类
 */

public class JDBCUtils {

    //获取数据库连接
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//        final String defaultConnectIP = "172.20.3.27";
        String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
        String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
        String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765/db?timeout=3600";

        //加载驱动
        Class.forName(JDBC_DRIVER);

        //获取连接
        Connection conn = DriverManager.getConnection(connectUrl);
        return conn;
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
