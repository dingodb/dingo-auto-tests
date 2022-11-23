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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LikeState {
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

    //向表中插入数据
    public void insertTableValues(String tableName, String insertFields, String tableValues) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "insert into " + tableName + insertFields + " values " + tableValues;
            statement.execute(sql);
        }
    }

    //查询数据,返回数据列表
    public List<List> likeQueryList(String tableName, String queryFields, String queryState, int fieldsCnt, String outFields) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select " + queryFields + " from " + tableName + " " + queryState;
            ResultSet resultSet = statement.executeQuery(sql);
            List<List> queryList = new ArrayList<List>();
            int queryColNum = new ArrayList(Arrays.asList(queryFields.split(","))).size();
            List<String> testFieldsList = new ArrayList(Arrays.asList(outFields.split(",")));
            while (resultSet.next()) {
                List rowList = new ArrayList();
                for (int i = 0;i<testFieldsList.size(); i++) {
                    rowList.add(resultSet.getString(testFieldsList.get(i)));
                }

//                if(queryFields.equals("*")) {
//                    for(int i = 1; i <= fieldsCnt; i++){
//                        rowList.add(resultSet.getString(i));
//                    }
//                } else {
//                    for (int j=1; j <= queryColNum; j++) {
//                        rowList.add(resultSet.getString(j));
//                    }
//                }
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //查询数据,返回数据总行数
    public int likeQueryCount(String tableName, String queryFields, String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select " + queryFields + " from " + tableName + " " + queryState;
            ResultSet resultSet = statement.executeQuery(sql);
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount ++;
            }
            resultSet.close();
            statement.close();
            return rowCount;
        }
    }

}
