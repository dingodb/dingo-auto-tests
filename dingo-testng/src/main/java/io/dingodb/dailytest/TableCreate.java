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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableCreate {
    //    private static final String defaultConnectIP = "172.20.3.27";
//    private static final String defaultConnectIP = "172.20.61.1";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";
    public static Connection connection = null;

    static{
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(connectUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //创建测试表1,所有字段均不为null，有默认值
    public void createTableWithDefaultValue(String table1_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest001 " + table1_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表1只插入id字段，验证默认值
    public void insertTable1Values(String table1_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest001(id) values " +  table1_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表1数据
    public List<List> queryTable1Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest001";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }

    //创建测试表2, varchar类型字段为主键
    public void createTablePrimaryKeyVarchar(String table2_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest002 " + table2_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表2插入数据，varchar主键有重复数据只允许插入一个
    public void insertTable2Values(String table2_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest002 values " +  table2_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表2数据
    public List<List> queryTable2Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest002";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }


    //创建测试表3, double类型字段为主键
    public void createTablePrimaryKeyDouble(String table3_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest003 " + table3_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表3插入数据，double主键有重复数据只允许插入一个
    public void insertTable3Values(String table3_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest003 values " +  table3_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表3数据
    public List<List> queryTable3Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest003";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }

    //创建测试表4, date类型字段为主键
    public void createTablePrimaryKeyDate(String table4_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest004 " + table4_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表4插入数据，date主键有重复数据只允许插入一个
    public void insertTable4Values(String table4_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest004 values " +  table4_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表4数据
    public List<List> queryTable4Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest004";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }

    //创建测试表5, time类型字段为主键
    public void createTablePrimaryKeyTime(String table5_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest005 " + table5_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表5插入数据，time主键有重复数据只允许插入一个
    public void insertTable5Values(String table5_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest005 values " +  table5_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表5数据
    public List<List> queryTable5Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest005";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }

    //创建测试表6, timestamp类型字段为主键
    public void createTablePrimaryKeyTimestamp(String table6_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest006 " + table6_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表6插入数据，timestamp主键有重复数据只允许插入一个
    public void insertTable6Values(String table6_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest006 values " +  table6_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表6数据
    public List<List> queryTable6Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest006";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }

    //创建测试表7, 布尔类型字段为主键
    public void createTablePrimaryKeyBoolean(String table7_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table ctest007 " + table7_Meta;
            statement.execute(createTableSQL);
        }
    }

    //测试表7插入数据，布尔主键有重复数据只允许插入一个
    public void insertTable7Values(String table7_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into ctest007 values " +  table7_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //获取表7数据
    public List<List> queryTable7Data() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from ctest007";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("is_delete"));
                queryList.add(rowList);
            }
            statement.close();
            return queryList;
        }
    }




}
