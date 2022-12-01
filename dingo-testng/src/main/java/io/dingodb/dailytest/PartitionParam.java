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
import java.sql.SQLException;
import java.sql.Statement;

public class PartitionParam {
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
}
