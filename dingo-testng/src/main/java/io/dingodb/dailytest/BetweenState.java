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

public class BetweenState {
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

    //创建between and语句测试表1
    public void betweenTable1Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table betweenTest " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表1插入数据
    public void insertTable1Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into betweenTest values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表2
    public void betweenTable2Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table betweenTest2 " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表2插入数据
    public void insertTable2Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into betweenTest2 values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表3
    public void betweenTable3Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table betweenTest3 " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表3插入数据
    public void insertTable3Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into betweenTest3 values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表4
    public void betweenTable4Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table betweenTest4 " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表4插入数据
    public void insertTable4Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into betweenTest4 values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表5
    public void betweenTable5Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table betweenTest5 " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表5插入数据
    public void insertTable5Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into betweenTest5 values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表6
    public void betweenTable6Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table between_employees " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表6插入数据
    public void insertTable6Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into between_employees values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }

    //创建between and语句测试表7
    public void betweenTable7Create(String between_and_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table between_job_grades " + between_and_Meta;
            statement.execute(createTableSQL);
        }
    }

    //表7插入数据
    public void insertTable7Values(String between_and_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into between_job_grades values " + between_and_Values;
            statement.execute(insertValuesSQL);
        }
    }


    //betwwen按主键id进行范围查询
    public List<List> betweenQueryByPrimaryKeyRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where id between 3 and 7";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not betwwen按主键id进行范围查询
    public List<List> notBetweenQueryByPrimaryKeyRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where id not between 3 and 17";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按整型字段值进行范围查找
    public List<List> betweenQueryByIntRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where age between 20 and 60";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按整型字段值进行范围查找
    public List<List> notBetweenQueryByIntRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where age not between 20 and 60";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getInt("age"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //按浮点型字段值进行范围查找
    public List<List> betweenQueryByDoubleRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where amount between 100.00 and 2000.00";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("amount"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按浮点型字段值进行范围查找
    public List<List> notBetweenQueryByDoubleRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where amount not between 100.00 and 2000.00";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getInt("amount"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按字符型字段值进行范围查找
    public List<List> betweenQueryByStrRange1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where name between 'b' and 'o'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按字符型字段值进行范围查找
    public List<List> notBetweenQueryByStrRange1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where name not between 'b' and 'o'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按字符型字段值进行范围查找
    public List<List> betweenQueryByStrRange2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where name between '2' and 'a'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按字符型字段值进行范围查找
    public List<List> notBetweenQueryByStrRange2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where name not between '2' and 'a'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按字符型字段值进行范围查找
    public List<List> betweenQueryByStrRange3() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where address between 'beijing ' and 'JiZhou'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("address"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按字符型字段值进行范围查找
    public List<List> notBetweenQueryByStrRange3() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where address not between 'beijing ' and 'JiZhou'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("address"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按Date类型字段值进行范围查找
    public List<List> betweenQueryByDateRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where birthday between '2010-10-01' and '2020-11-11'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按Date类型字段值进行范围查找
    public List<List> notBetweenQueryByDateRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where birthday not between '2010-10-01' and '2020-11-11'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按time类型字段值进行范围查找
    public List<List> betweenQueryByTimeRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where create_time between '06:00:00' and '19:00:00'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("create_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按time类型字段值进行范围查找
    public List<List> notBetweenQueryByTimeRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where create_time not between '06:00:00' and '19:00:00'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("create_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between按timestamp类型字段值进行范围查找
    public List<List> betweenQueryByTimeStampRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time between '2000-01-01 00:00:00' and '2022-07-07 13:30:03'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between按timestamp类型字段值进行范围查找
    public List<List> notBetweenQueryByTimeStampRange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time not between '2000-01-01 00:00:00' and '2022-07-07 13:30:03'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between起始值大于终止值，返回空
    public Boolean betweenQueryStartGTEnd(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between起始值大于终止值，返回全部数据
    public int notBetweenQueryStartGTEnd(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            int rowNum = 0;
            while (resultSet.next()) {
                rowNum++;
            }
            statement.close();
            return rowNum;
        }
    }

    //between起止范围超过数据实际范围返回全部数据
    public int betweenQueryByFullRange(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            int rowNum = 0;
            while (resultSet.next()) {
                rowNum++;
            }
            statement.close();
            return rowNum;
        }
    }

    //not between起止范围超过数据实际范围返回空
    public Boolean notBetweenQueryByFullRange(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }


    //between无效的时间日期值，返回空
    public Boolean betweenQueryInvalidDateTime(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest " + queryState;
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between无效的起止时间日期值，返回空
    public Boolean notBetweenQueryInvalidDateTime(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest " + queryState;
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between起始日期无效，按另一个有效日期的区间返回数据
    public List<List> notBetweenQueryInvalidStartDate() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where birthday not between '1995-12-32' and '2010-10-01'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between终止日期无效，按另一个有效日期的区间返回数据
    public List<List> notBetweenQueryInvalidEndDate() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where birthday not between '2000-01-01' and '2010-13-01'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between起始时间无效，按另一个有效时间的区间返回数据
    public List<List> notBetweenQueryInvalidStartTime() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where create_time not between '10:30:60' and '12:30:30'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("create_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between终止时间无效，按另一个有效时间的区间返回数据
    public List<List> notBetweenQueryInvalidEndTime() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where create_time not between '12:00:00' and '20:30:60'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("create_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between单个起止timestamp无效，按另一个有效timestamp的区间返回数据
    public List<List> notBetweenQueryInvalidStartTimestamp1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time not between '2000-02-30 00:00:00' and '2010-10-11 23:00:00'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between单个起止timestamp无效，按另一个有效timestamp的区间返回数据
    public List<List> notBetweenQueryInvalidStartTimestamp2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time not between '2020-12-31 24:00:00' and '2022-12-31 12:30:30'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between单个起止timestamp无效，按另一个有效timestamp的区间返回数据
    public List<List> notBetweenQueryInvalidEndTimestamp1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time not between '2000-02-29 00:00:00' and '2010-10-11 24:00:00'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between单个起止timestamp无效，按另一个有效timestamp的区间返回数据
    public List<List> notBetweenQueryInvalidEndTimestamp2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where update_time not between '2010-12-29 12:00:00' and '2010-04-31 12:00:00'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("update_time"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }


    //between查询范围未匹配到数据，返回空集
    public Boolean betweenQueryNoValueMatched(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between查询范围未匹配到数据，返回空集
    public Boolean notBetweenQueryNoValueMatched(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //between查询日期范围，日期格式无效，预期失败
    public Boolean betweenQueryNotSupportDateFormat(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between查询日期范围，日期格式无效，预期失败
    public Boolean notBetweenQueryNotSupportDateFormat(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //查询范围语句参数不符合要求，预期失败
    public Boolean queryIncorrectParam(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //between查询当任意参数为Null返回空集
    public Boolean betweenQueryNullValue(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //not between查询当起始值其中一个为null，返回另一个非null值的范围数据
    public List<List> notBetweenQuerySingleNullValue1() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest2 where age not between null and 18";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList<>();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between查询当起始值其中一个为null，返回另一个非null值的范围数据
    public List<List> notBetweenQuerySingleNullValue2() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String betweenSQL = "select id,name from betweenTest2 where id not between 2 and null";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));;
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between查询当起始值其中一个为null，返回另一个非null值的范围数据
    public List<List> notBetweenQuerySingleNullValue3() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String betweenSQL = "select id,name,age from betweenTest2 where name not between null and 'li3'";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));;
                rowList.add(resultSet.getString("age"));;
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between查询当起止参数均为Null返回空集
    public Boolean notBetweenQueryBothNullValue(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //between查询当条件字段值均为Null返回空集
    public Boolean queryColumnNullValue(String queryState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            Boolean queryResult = resultSet.next();
            statement.close();
            return queryResult;
        }
    }

    //Between字段部分值为null
    public List<List> betweenQueryColumnPartNull() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest4 where age between 18 and 30";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //Not Between字段部分值为null
    public List<List> notBetweenQueryColumnPartNull() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest4 where age not between 18 and 30";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //Between查询起止值相同
    public List<List> betweenQueryStartEQEnd(String queryState, String testFieldStr) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            String[] testFieldArray = testFieldStr.split(",");
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                for(int i = 0; i < testFieldArray.length; i++){
                    rowList.add(resultSet.getString(testFieldArray[i]));
                }
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not Between查询起止值相同
    public List notBetweenQueryStartEQEnd(String queryState, String testFieldStr) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryState);
            List betweenList = new ArrayList();
            while (resultSet.next()) {
                betweenList.add(resultSet.getString(testFieldStr));
            }
            statement.close();
            return betweenList;
        }
    }

    //多个between and使用
    public List<List> multiBetweenQuery() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where id between 5 and 20 and age between 20 and 50 and " +
                    "birthday between '2000-01-01' and '2022-01-01' and is_delete<>false";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getTime(7).toString());
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //多个not between and使用
    public List<List> multiNotBetweenQuery() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where id not between 1 and 10 and amount not between 0 and" +
                    " 100 and create_time not between '00:00:00' and '12:00:00' and name in ('Kelay','Juliya','Adidas')" +
                    " and is_delete is false";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getTime(7).toString());
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between 和 not between 一起使用,且关系
    public List<List> betweenAndNotBetweenQueryTogether1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where id between 1 and 10 and update_time not between" +
                    " '2000-02-29 00:00:00' and '2010-12-31 23:59:59' order by update_time limit 3";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getTime(7).toString());
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between 和 not between 一起使用，或关系
    public List<List> betweenAndNotBetweenQueryTogether2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select * from betweenTest where ((age not between 10 and 100) or " +
                    "(address between 'a' and 'c') or (birthday between '1980-01-01' and '1990-01-01')) and " +
                    "locate('beijing',address)>0 and name not in ('zhangsan') order by is_delete desc";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getTime(7).toString());
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //between查询使用聚合函数
    public List betweenQueryWithAggrFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select avg(age) aa,sum(amount) sa,min(birthday) mb from betweenTest " +
                    "where id between 10 and 20";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List betweenList = new ArrayList();
            while (resultSet.next()) {
                betweenList.add(resultSet.getString("aa"));
                betweenList.add(resultSet.getString("sa"));
                betweenList.add(resultSet.getString("mb"));
            }
            statement.close();
            return betweenList;
        }
    }

    //not between查询使用聚合函数
    public List notBetweenQueryWithAggrFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select format(avg(amount),2) aa,format(sum(amount),2) sa,max(create_time) mc," +
                    "min(update_time) mu from betweenTest where age not between 0 and 18";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List betweenList = new ArrayList();
            while (resultSet.next()) {
                betweenList.add(resultSet.getString("aa"));
                betweenList.add(resultSet.getString("sa"));
                betweenList.add(resultSet.getTime("mc").toString());
                betweenList.add(resultSet.getString("mu"));
            }
            statement.close();
            return betweenList;
        }
    }

    //between查询使用分组
    public List<List> betweenQueryWithGroup() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select address,sum(amount) sa from betweenTest where address between 'a' and 'd' group by address";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("sa"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //not between查询使用分组
    public List<List> notBetweenQueryWithGroup() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenSQL = "select address,avg(age) aa from betweenTest where id not between 10 and 21 and " +
                    "id in (1,3,5,7,8) group by address order by aa";
            ResultSet resultSet = statement.executeQuery(betweenSQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("aa"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在update语句中使用-字符型字段更新
    public List<List> betweenInUpdateCharState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set name='Java', address='BJ' where id between 1 and 5";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,age,address from betweenTest5 where id between 1 and 5";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在update语句中使用-数值型字段更新
    public List<List> betweenInUpdateNumState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set age=35, amount=1234.5678 where id between 6 and 10";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,age,amount from betweenTest5 where id between 6 and 10";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("amount"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在update语句中使用-日期时间型字段更新
    public List<List> betweenInUpdateDateTimeState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set birthday='2022-07-13', create_time='11:48:06', " +
                    "update_time='2022-08-01 00:00:00' where id between 11 and 15";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,birthday,create_time,update_time from betweenTest5 where id between 11 and 15";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在update语句中使用-布尔型字段更新
    public List<List> betweenInUpdateBooleanState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set is_delete=true where id between 16 and 20";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,id_delete from betweenTest5 where id between 16 and 20";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("id_delete"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在update语句中使用-更新范围为空集
    public int betweenUpdateEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set name='Python', address='DL' where id between 25 and 30";
            int updateRows = statement.executeUpdate(betweenUpdateSQL);
            return updateRows;
        }
    }

    //验证not between在update语句中使用-字符型字段更新
    public List<List> notBetweenInUpdateCharState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set name='C++', address='SH' where id not between 1 and 5";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,age,address from betweenTest5 where id not between 1 and 5";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("address"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在update语句中使用-数值型字段更新
    public List<List> notBetweenInUpdateNumState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set age=66, amount=7890.0123 where id not between 6 and 10";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,age,amount from betweenTest5 where id not between 6 and 10";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("amount"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在update语句中使用-日期时间型字段更新
    public List<List> notBetweenInUpdateDateTimeState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set birthday='1987-06-18', create_time='23:59:59', " +
                    "update_time='2022-07-31 12:00:00' where id not between 11 and 15";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,birthday,create_time,update_time from betweenTest5 where id not between 11 and 15";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在update语句中使用-布尔型字段更新
    public List<List> notBetweenInUpdateBooleanState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set is_delete=true where id not between 16 and 20";
            statement.executeUpdate(betweenUpdateSQL);

            String betweenQuerySQL = "select id,name,id_delete from betweenTest5 where id not between 16 and 20";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("id_delete"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在update语句中使用-更新范围为空集
    public int notBetweenUpdateEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenUpdateSQL = "update betweenTest5 set name='Delphi',age=33 where age not between 1 and 200";
            int updateRows = statement.executeUpdate(betweenUpdateSQL);
            return updateRows;
        }
    }

    //验证between在delete语句中使用-删除范围不为空
    public int betweenDeleteNotEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenDeleteSQL = "delete from betweenTest5 where id between 1 and 5";
            int deleteRows = statement.executeUpdate(betweenDeleteSQL);
            return deleteRows;
        }
    }

    //验证between删除后查询数据
    public List<List> betweenQueryAfterDelete() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenQuerySQL = "select * from betweenTest5";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("id_delete"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在delete语句中使用-删除范围不为空
    public int notBetweenDeleteNotEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenDeleteSQL = "delete from betweenTest5 where id not between 6 and 10";
            int deleteRows = statement.executeUpdate(betweenDeleteSQL);
            return deleteRows;
        }
    }

    //验证not between删除后查询数据
    public List<List> notBetweenQueryAfterDelete() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenQuerySQL = "select * from betweenTest5";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("id"));
                rowList.add(resultSet.getString("name"));
                rowList.add(resultSet.getString("age"));
                rowList.add(resultSet.getString("amount"));
                rowList.add(resultSet.getString("address"));
                rowList.add(resultSet.getString("birthday"));
                rowList.add(resultSet.getTime("create_time").toString());
                rowList.add(resultSet.getString("update_time"));
                rowList.add(resultSet.getString("id_delete"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证between在delete语句中使用-删除范围为空集
    public int betweenDeleteEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenDeleteSQL = "delete from betweenTest5 where id between 25 and 30";
            int deleteRows = statement.executeUpdate(betweenDeleteSQL);
            return deleteRows;
        }
    }

    //验证not between在delete语句中使用-删除范围为空集
    public int notBetweenDeleteEmpSet() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenDeleteSQL = "delete from betweenTest5 where id not between 1 and 21";
            int deleteRows = statement.executeUpdate(betweenDeleteSQL);
            return deleteRows;
        }
    }

    //验证between在非等值连接中使用
    public List<List> betweenInInnerJoin() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenQuerySQL = "select employee_id,salary,grade_level from between_employees e join between_job_grades g " +
                    "on e.salary between g.lowest_sal and g.highest_sal";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("employee_id"));
                rowList.add(resultSet.getString("salary"));
                rowList.add(resultSet.getString("grade_level"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

    //验证not between在非等值连接中使用
    public List<List> notBetweenInInnerJoin() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String betweenQuerySQL = "select salary,grade_level from between_employees e join between_job_grades g " +
                    "on e.salary not between g.lowest_sal and g.highest_sal";
            ResultSet resultSet = statement.executeQuery(betweenQuerySQL);
            List<List> betweenList = new ArrayList<List>();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString("salary"));
                rowList.add(resultSet.getString("grade_level"));

                betweenList.add(rowList);
            }
            statement.close();
            return betweenList;
        }
    }

}
