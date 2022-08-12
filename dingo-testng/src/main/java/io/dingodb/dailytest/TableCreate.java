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


}
