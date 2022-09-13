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

package io.dingodb.dailytest;

import io.dingodb.common.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class DailyBVT {
//    private static final String defaultConnectIP = "172.20.3.27";
//    private static final String defaultConnectIP = "172.20.61.1";
//    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
//    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
//    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";
    public static Connection connection = null;

    public String inName = "tomy";
    public String newName = "new1";

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //获取数据库连接
//    public static Connection connectDingo() throws ClassNotFoundException, SQLException, IOException {
//        InputStream is = DailyBVT.class.getClassLoader().getResourceAsStream("jdbc.properties");
//        Properties pros = new Properties();
//        pros.load(is);
//        String JDBC_DRIVER = pros.getProperty("JDBC_Driver");
//        Class.forName(JDBC_DRIVER);
//        connection = DriverManager.getConnection(connectUrl);
//        return connection;
//    }

    //获取表名方法，返回BVTTest + 当前日期的表名
    public String getTableName() {
        final String tablePrefix = "BVTTest";
//        Date date = new Date();
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        String dateNowStr = sdf.format(date);
        String tableName = tablePrefix + CommonArgs.getCurDateStr();
        return tableName;
    }

    //创建表
    public void createTable() throws Exception {
//        connection = connectDingo();
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "primary key(id)"
                    + ")";
            statement.execute(sql);
        }
    }

    //插入单行数据
    public int insertTableValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            int startID = 1;
            Random r = new Random();
            int inAge = r.nextInt(100);
            float amount = r.nextFloat() * 100;
            String insertSql = "insert into " + tableName + " values (" + startID + ",'"  + inName + "'," + inAge + "," + amount + ")";
            int insertCount = statement.executeUpdate(insertSql);
            statement.close();
            return insertCount;
        }
    }

    //更新单行字符型数据
    public int updateStringValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            String updateSql = "update " + tableName + " set name='" + newName + "' where name='" + inName + "'";
            int updateStrCount = statement.executeUpdate(updateSql);
            statement.close();
            return updateStrCount;
        }
    }

    //更新单行整型数据
    public int updateIntValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            String updateSql = "update " + tableName + " set age = age + 1";
            int updateIntCount = statement.executeUpdate(updateSql);
            statement.close();
            return updateIntCount;

        }
    }

    //更新后查询数据
    public String queryTable() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        String queryName = null;
        try(Statement statement = connection.createStatement()) {
            String querySql = "select * from " + tableName + " where name='" + newName + "'";
            ResultSet querySet = statement.executeQuery(querySql);
            while (querySet.next()) {
                queryName = querySet.getString(2);
            }
            querySet.close();
            statement.close();
            return queryName;
        }
    }

    //清空表
    public int deleteTableValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            String deleteSql = "delete from " + tableName;
            int deleteCount = statement.executeUpdate(deleteSql);
            statement.close();
            return deleteCount;
        }
    }

    //删除表
    public void dropTable() throws Exception {
        String tableName = getTableName();
        try(Statement statement = connection.createStatement()) {
            String sql = "drop table " + tableName;
            statement.execute(sql);
        }
    }
}
