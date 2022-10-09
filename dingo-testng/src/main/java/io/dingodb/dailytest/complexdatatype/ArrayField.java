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
                    " (id int,name varchar(32)," + arrayField + " " + arrayType + " array,primary key(id))";

            statement.execute(sql);
        }
    }

    //向表中插入array数组数据
    public void insertArrayValues(String tableName, String arrayValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "insert into " + tableName + " values " + arrayValue;
            statement.execute(sql);
        }
    }

    //查询插入后的数据
    public List<List> queryTableData(String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from " + tableName + " where id<=5 or id=19121 or id=19251";
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

    //查询插入的array字段的数据
    public List<List> queryDataWithCondition(String tableName, String queryLogic) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from " + tableName + " where " + queryLogic;
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

    //查询插入的array字段值为null
    public List<List> queryDataNull(String tableName, String queryLogic) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from " + tableName + " where " + queryLogic;
            ResultSet resultSet = statement.executeQuery(sql);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));

                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //创建含有多种array类型字段的表
    public void tableCreateWithMixArrayColumn(String tableName, String tableMeta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName + tableMeta;
            statement.execute(sql);
        }
    }

    //向多种array类型字段表中插入数据
    public int insertValuesToMixArrayTable(String tableName, String tableValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "insert into " + tableName + " values " + tableValue;
            int insertRow = statement.executeUpdate(sql);
            return insertRow;
        }
    }

    //array类型字段值作为查询条件，预期失败
    public void queryByArrayValue() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from mixarray1 where class_no=array[10, 200, 3000]";
            statement.executeQuery(sql);
        }
    }

    //删除含有array数组类型的数据
    public int deleteSingleRecordWithArrayColumn(String conditions) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "delete from mixarray1" + conditions;
            int deleteRow = statement.executeUpdate(sql);
            return deleteRow;
        }
    }

    //删除所有数据后查询
    public boolean queryAfterDeleteAll() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from mixarray1";
            ResultSet resultSet = statement.executeQuery(sql);
            return resultSet.next();
        }
    }

    //查询arraymid,arrayfirst表数据，array字段在表中间列,首列
    public List<List> queryDataInMidFirstArrayTbl(String queryFields, String tableName, String queryLogic, int fieldNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select " + queryFields + " from " + tableName + queryLogic;
            ResultSet resultSet = statement.executeQuery(sql);
            List<List> queryList = new ArrayList<List>();
            int queryColNum = new ArrayList(Arrays.asList(queryFields.split(","))).size();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                if(queryFields.equals("*")) {
                    for(int i = 1; i <= fieldNum; i++){
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

    //array字段允许为Null，插入数据不指定array字段
    public List<List> insertToTableWithoutArrayColAllowNull() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql = "create table arrayNu(id int, name varchar(20), age int, amount double, user_info varchar array, primary key(id))";
            statement.execute(createSql);
            String insertSql = "insert into arrayNu(id,name,age,amount) values(1,'zhangsan',55,23.45)";
            statement.executeUpdate(insertSql);
            String querySql = "select * from arrayNu";
            ResultSet resultSet = statement.executeQuery(querySql);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //array字段不允许为Null，插入数据不指定array字段，预期失败
    public void insertToTableWithoutArrayColNotNull() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql = "create table arrayNotNu(id int, name varchar(20), age int, user_info varchar array not null, primary key(id))";
            statement.execute(createSql);
            String insertSql = "insert into arrayNotNu(id,name,age) values(1,'zhangsan',55)";
            statement.executeUpdate(insertSql);
        }
    }

    //删除arrray字段值为null或元素为null的行
    public int deleteArrayNullRecords(String tableName, String deleteLogic) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "delete from " + tableName + deleteLogic;
            int deleteRows = statement.executeUpdate(sql);
            return deleteRows;
        }
    }

    //array为null的行删除后查看表数据
    public boolean queryAfterDeleteNull() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "select * from atest1903";
            ResultSet resultSet = statement.executeQuery(sql);
            return resultSet.next();
        }
    }

    //指定array字段插入数据
    public List insertToTableWithArrayColSpecified() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSql = "insert into arrayNu(id,user_info) values(2186,array['test1','hg','177'])";
            statement.executeUpdate(insertSql);
            String querySql = "select * from arrayNu where id=2186";
            ResultSet resultSet = statement.executeQuery(querySql);
            List queryList = new ArrayList();
            while (resultSet.next()) {
                queryList.add(resultSet.getString(1));
                queryList.add(resultSet.getString(2));
                queryList.add(resultSet.getString(3));
                queryList.add(resultSet.getString(4));
                queryList.add(resultSet.getString(5));
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //array更新
    public int arrayUpdate(String updateState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            int updateRows = statement.executeUpdate(updateState);
            return updateRows;
        }
    }

    //array更新后查看数据
    public List<List> queryAfterArrayUpdate(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            List<List> queryList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }
}
