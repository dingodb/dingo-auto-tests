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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BooleanField {
    public static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //创建表
    public void tableCreate(String tableName, String tableMeta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName + tableMeta;
            statement.execute(sql);
        }
    }

    //插入数据返回行数
    public int insertTableValues(String tableName,String insertFields, String tableValues) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSql = "insert into " + tableName + insertFields + " values " + tableValues;
            int insertRows = statement.executeUpdate(insertSql);
            statement.close();
            return insertRows;
        }
    }

    // 查看布尔数据
    public List<Boolean> getBoolValues(String tableName, String queryFields, String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select " + queryFields + " from " + tableName + queryState;
            ResultSet queryRst = statement.executeQuery(querySql);
            List<Boolean> queryList = new ArrayList<Boolean>();
            while (queryRst.next()) {
                queryList.add(queryRst.getBoolean("is_delete"));
            }
            queryRst.close();
            statement.close();
            return queryList;
        }
    }

    // 查看条件查询数据
    public List queryValue(String tableName, String queryFields, String queryState, String queryColumn) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String queryTrueValueSql = "select " + queryFields + " from " + tableName + queryState;
            ResultSet queryTrueValueRst = statement.executeQuery(queryTrueValueSql);
            List queryTrueValueList = new ArrayList();
            while (queryTrueValueRst.next()) {
                queryTrueValueList.add(queryTrueValueRst.getString(queryColumn));
            }

            queryTrueValueRst.close();
            statement.close();
            return queryTrueValueList;
        }
    }

    // 查看0转换后数据
    public Boolean getZeroValues(String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String queryZeroSql = "select * from " + tableName + " where id=35";
            ResultSet queryZeroRst = statement.executeQuery(queryZeroSql);
            Boolean zeroOut = null;
            while (queryZeroRst.next()) {
                zeroOut = queryZeroRst.getBoolean("is_delete");
            }

            queryZeroRst.close();
            statement.close();
            return zeroOut;
        }
    }

    //插入正整数转换为true
    public int insertPosIntegerValues(String tableName, String insertID, String insertIntegerValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertPosIntegerSql = "insert into " + tableName + " values "
                    + "(" + insertID + ",'oppo',20,1.2,'wuhan'," + insertIntegerValue + ")";

            int insertIntegerRows = statement.executeUpdate(insertPosIntegerSql);
            statement.close();
            return insertIntegerRows;
        }
    }

    // 查看正整数转换后数据
    public Boolean getIntegerValues(String tableName, String insertID) throws SQLException {
        try(Statement statement = connection.createStatement()){
            String queryIntegerSql = "select * from " + tableName + " where id = " + insertID;
            ResultSet queryIntegerRst = statement.executeQuery(queryIntegerSql);
            Boolean integerOut = null;
            while (queryIntegerRst.next()) {
                integerOut = queryIntegerRst.getBoolean("is_delete");
            }

            queryIntegerRst.close();
            statement.close();
            return integerOut;
        }
    }

    //布尔字段插入null值
    public String insertNull(String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()){
            String insertSql = "insert into " + tableName + " values(10001, 'kelay',76,2.30,'shanghai', null)";
            int insertNum = statement.executeUpdate(insertSql);
            String querySql = "select * from " + tableName + " where id=10001";
            ResultSet resultSet = statement.executeQuery(querySql);
            String integerOut = null;
            while (resultSet.next()) {
                integerOut = resultSet.getString("is_delete");
            }

            resultSet.close();
            statement.close();
            return integerOut;
        }
    }

}
