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

package io.dingodb.dailytest.compositedatatype;

import io.dingodb.common.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ArrayField {
    public static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建含有Array类型字段的表,不指定默认值
    public void tableCreateWithArrayField(String tableName, String arrayField, String arrayType) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName +
                    " (id int,name varchar(32)," + arrayField + " " + arrayType + " array ,primary key(id))";

            statement.execute(sql);
        }
    }

    //查询插入后的数据
    public List<List> queryTableData(String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from " + tableName;
            ResultSet resultSet = statement.executeQuery(sql);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getArray(3).toString());

                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }


    //向表中插入array数组数据
    public void insertArrayValues(String tableName, String arrayValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "insert into " + tableName + " values " + arrayValue;
            statement.execute(sql);
        }
    }

    //创建含有Array类型字段的表，指定默认值
    public void tableCreateWithArrayFieldDefaultValue(String tableName, String arrayFields, String arrayType, String defaultValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName +
                    " (id int,name varchar(32)," + arrayFields + " " + arrayType + " array not null default array" + defaultValue + ",primary key(id))";

            statement.execute(sql);
        }
    }

    //向含有数组默认值的表中插入除数组字段外的其他字段的数据
    public List insertValuesWithoutArrayField(String tableName, String typeFields, String insertValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql1 = "insert into " + tableName + "(" + typeFields + ") values " + insertValue;
            statement.execute(sql1);

            String sql2 = "select * from " + tableName;
            ResultSet resultSet = statement.executeQuery(sql2);
            List queryList= new ArrayList();
            while(resultSet.next()){
                queryList.add(resultSet.getString(1));
                queryList.add(resultSet.getString(2));
                queryList.add(resultSet.getString(3));
                queryList.add(resultSet.getArray(4).toString());
            }

            resultSet.close();
            return queryList;
        }


    }



}
