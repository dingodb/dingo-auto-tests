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

import java.sql.*;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Random;

public class DailyBVT {
    private static final String defaultConnectIP = "172.20.3.26";
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=http://" + defaultConnectIP + ":8765";

    private static Connection connection;
    public String inName = "tomy";
    public String newName = "new1";

    //创建连接方法，并返回连接对象
    public Connection connectDingo() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(connectUrl);
        return connection;
    }

    //获取表名方法，返回BVTTest + 当前日期的表名
    public String getTableName() {
        final String tablePrefix = "BVTTest";
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateNowStr = sdf.format(date);
        String tableName = tablePrefix + dateNowStr;
        return tableName;
    }

    //创建表
    public void createTable() throws Exception {
        String tableName = getTableName();
        connection = connectDingo();
        Statement statement = connection.createStatement();

        String sql = "create table " + tableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "primary key(id)"
                + ")";
        statement.execute(sql);
        statement.close();
        connection.close();
    }

    //插入单行数据
    public int insertTableValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        connection = connectDingo();
        Statement statement = connection.createStatement();

        int startID = 1;
        Random r = new Random();
        int inAge = r.nextInt(100);
        float amount = r.nextFloat() * 100;
        String insertSql = "insert into " + tableName + " values (" + startID + ",'"  + inName + "'," + inAge + "," + amount + ")";
        int insertCount = statement.executeUpdate(insertSql);
        statement.close();
        connection.close();
        return insertCount;
    }

    //更新单行数据
    public int updateTableValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        connection = connectDingo();
        Statement statement = connection.createStatement();

        String updateSql = "update " + tableName + " set name='" + newName + "' where name='" + inName + "'";
        int updateCount = statement.executeUpdate(updateSql);
        statement.close();
        connection.close();
        return updateCount;
    }

    //更新后查询数据
    public String queryTable() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        String queryName = null;
        connection = connectDingo();
        Statement statement = connection.createStatement();

        String querySql = "select * from " + tableName + " where name='" + newName + "'";
        ResultSet querySet = statement.executeQuery(querySql);
        while (querySet.next()) {
            queryName = querySet.getString(2);
        }
        statement.close();
        connection.close();
        return queryName;
    }

    //清空表
    public int deleteTableValues() throws ClassNotFoundException, SQLException {
        String tableName = getTableName();
        connection = connectDingo();
        Statement statement = connection.createStatement();

        String deleteSql = "delete from " + tableName;
        int deleteCount = statement.executeUpdate(deleteSql);
        statement.close();
        connection.close();
        return deleteCount;
    }

    //删除表
    public void dropTable() throws Exception {
        String tableName = getTableName();
        connection = connectDingo();
        Statement statement = connection.createStatement();

        String sql = "drop table " + tableName;
        statement.execute(sql);
        statement.close();
        connection.close();
    }
}