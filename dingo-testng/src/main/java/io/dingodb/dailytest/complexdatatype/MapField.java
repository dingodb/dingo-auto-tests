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

package io.dingodb.dailytest.complexdatatype;

import io.dingodb.common.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapField {
    public static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建含有Map类型字段的表,不指定默认值
    public void tableCreateWithMapField(String tableName, String tableMeta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName +  tableMeta;
            statement.execute(sql);
        }
    }

    //向maptest表中的map字段插入键值
    public void insertKV(String tableName, String insertFields, String tableValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "insert into " + tableName + insertFields + " values " + tableValue;
            statement.executeUpdate(sql);
        }
    }

    //查询插入后的数据
    public List<List> queryTableData(String tableName, String queryFields, String queryState, int fieldsCnt) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select " + queryFields + " from " + tableName + queryState;
            ResultSet resultSet = statement.executeQuery(sql);
            List<List> queryList = new ArrayList<List>();
            int queryColNum = new ArrayList(Arrays.asList(queryFields.split(","))).size();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                if(queryFields.equals("*")) {
                    for(int i = 1; i <= fieldsCnt; i++){
                        rowList.add(resultSet.getString(i));
                    }
                } else {
                    for (int j=1; j <= queryColNum; j++) {
                        rowList.add(resultSet.getString(j));
                    }
                }
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //查询返回是否有结果集
    public Boolean queryResultSet(String tableName, String queryFields, String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select " + queryFields + " from " + tableName + " " + queryState;
            ResultSet resultSet = statement.executeQuery(sql);
            return resultSet.next();
        }
    }

    //更新表数据
    public int mapTableUpdateCommonCol(String updateState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            int affectRows = statement.executeUpdate(updateState);
            return affectRows;
        }
    }

    //删除数据
    public int deleteTableData(String tableName, String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "delete from " + tableName + " " + queryState;
            int deleteRows = statement.executeUpdate(sql);
            return deleteRows;
        }
    }

}
