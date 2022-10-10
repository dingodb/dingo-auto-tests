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
import java.util.List;

public class DefectCase {
    public static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void createTable0033() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSQL = "create table defect0033(id int, name varchar(20) not null, age int not null, amount double, " +
                    "address varchar(255), birthday date, create_time time, update_time timestamp, is_delete boolean, primary key (id))";
            statement.execute(createSQL);
        }
    }

    //插入的字段不含不允许为null的字段
    public void defect0033_1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
//            String createSQL = "create table defect0033(id int, name varchar(20) not null, age int not null, amount double, " +
//                    "address varchar(255), birthday date, create_time time, update_time timestamp, is_delete boolean, primary key (id))";
//            statement.execute(createSQL);
            String insertSQL = "insert into defect0033(id,name) values(331,'zhangsan')";
            statement.executeUpdate(insertSQL);
        }
    }

    //不允许为null的字段，插入值为null
    public void defect0033_2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSQL = "insert into defect0033 values(332,'zhangsan',null,121.35,'BJ','2021-06-13','18:00:00','2022-06-13 10:08:59', true)";
            statement.executeUpdate(insertSQL);
        }
    }

    //不插入主键,失败
    public void defect0033_3() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSQL = "insert into defect0033(name,age) values('zhangsan',18)";
            statement.executeUpdate(insertSQL);
        }
    }

    //验证直接算术运算
    public List<Integer> defect0046() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql1 = "select 1 + 1";
            String querySql2 = "select '1234' - 4321";
            String querySql3 = "select 12*'13'";
            String querySql4 = "select '1024'/'5'";
            String querySql5 = "select 1024/5";
            ResultSet resultSet1 = statement.executeQuery(querySql1);
            List<Integer> actualList = new ArrayList<Integer>();
            while(resultSet1.next()) {
                actualList.add(resultSet1.getInt(1));
            }

            ResultSet resultSet2 = statement.executeQuery(querySql2);
            while(resultSet2.next()) {
                actualList.add(resultSet2.getInt(1));
            }

            ResultSet resultSet3 = statement.executeQuery(querySql3);
            while(resultSet3.next()) {
                actualList.add(resultSet3.getInt(1));
            }

            ResultSet resultSet4 = statement.executeQuery(querySql4);
            while(resultSet4.next()) {
                actualList.add(resultSet4.getInt(1));
            }

            ResultSet resultSet5 = statement.executeQuery(querySql5);
            while(resultSet5.next()) {
                actualList.add(resultSet5.getInt(1));
            }

            resultSet1.close();
            resultSet2.close();
            resultSet3.close();
            resultSet4.close();
            resultSet5.close();
            statement.close();
            return actualList;
        }
    }

    //创建表时，字段重复，预期失败
    public void defect0136() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql = "create table defect0136(id int, name varchar(20), name varchar(10), age int, primary key(id))";
            statement.execute(createSql);
        }
    }

    //表格含有日期字段，只插入不允许为null的字段值
    public List defect0154() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSQL = "insert into defect0033(id,name,age) values(154,'zhangsan',18)";
            int effectRows = statement.executeUpdate(insertSQL);
            String querySQL = "select * from defect0033 where id=154";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List actualList = new ArrayList();
            actualList.add(String.valueOf(effectRows));
            while(resultSet.next()) {
                actualList.add(resultSet.getString(1));
                actualList.add(resultSet.getString(2));
                actualList.add(resultSet.getString(3));
                actualList.add(resultSet.getString(4));
                actualList.add(resultSet.getString(5));
                actualList.add(resultSet.getString(6));
                actualList.add(resultSet.getString(7));
                actualList.add(resultSet.getString(8));
                actualList.add(resultSet.getString(9));
            }

            resultSet.close();
            statement.close();
            return actualList;
        }
    }

    //表格含有日期字段，求时间日期字段的最大最小值
    public List defect0178_max_min() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSQL = "insert into defect0033 values " +
                    "(1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07', true),\n" +
                    "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00', false),\n" +
                    "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59', true),\n" +
                    "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00', true),\n" +
                    "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02', false),\n" +
                    "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12', false),\n" +
                    "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3', true),\n" +
                    "(8,'Gogo',86,23.45,'Shanghai','2022-03-13','12:00:00','2022-12-01 10:10:10', false)";
            statement.executeUpdate(insertSQL);
            String querySQL = "select max(birthday),max(create_time),max(update_time),min(birthday),min(create_time)," +
                    "min(update_time) from defect0033";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List actualList = new ArrayList();
            while(resultSet.next()) {
                actualList.add(resultSet.getString(1));
                actualList.add(resultSet.getString(2));
                actualList.add(resultSet.getString(3));
                actualList.add(resultSet.getString(4));
                actualList.add(resultSet.getString(5));
                actualList.add(resultSet.getString(6));
            }

            resultSet.close();
            statement.close();
            return actualList;
        }
    }

    //表格含有日期字段，按时间日期值进行排序
    public List<List> defect0178_orderByDate() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select id,birthday from defect0033 order by birthday";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> actualList = new ArrayList<List>();
            while(resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                actualList.add(rowList);
            }

            resultSet.close();
            statement.close();
            return actualList;
        }
    }

    //表格含有日期字段，按时间日期值进行排序
    public List<List> defect0178_orderByTime() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select id,create_time from defect0033 order by create_time desc";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> actualList = new ArrayList<List>();
            while(resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                actualList.add(rowList);
            }

            resultSet.close();
            statement.close();
            return actualList;
        }
    }

    //表格含有日期字段，按时间日期值进行排序
    public List<List> defect0178_orderByTimeStamp() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select id,update_time from defect0033 where update_time is not null order by update_time desc";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> actualList = new ArrayList<List>();
            while(resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                actualList.add(rowList);
            }

            resultSet.close();
            statement.close();
            return actualList;
        }
    }


    //更改字段值为非法数值，预期异常
    public void defect0186_1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String updateSQL = "update defect0033 set age='abc' where id=1";
            statement.execute(updateSQL);
        }
    }

    //更改字段值为非法数值，预期异常
    public void defect0186_2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String updateSQL = "update defect0033 set birthday='22-01-18' where id=2";
            statement.execute(updateSQL);
        }
    }

    //插入多条，时间日期类型字段为null,可插入成功
    public int defect0198() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSQL = "insert into defect0033 values " +
                    "(9,'Wula',71,98.20,'shanghai',null,'23:59:59','2022-06-13 18:36:35',0),\n" +
                    "(10,'Wula',71,98.20,'shanghai','1900-01-01',null,'2022-06-13 18:36:35',1),\n" +
                    "(11,'Wula',71,98.20,'shanghai','2000-12-31','00:00:00',null,2)";
            int effectRows = statement.executeUpdate(insertSQL);
            statement.close();
            return effectRows;
        }
    }
}
