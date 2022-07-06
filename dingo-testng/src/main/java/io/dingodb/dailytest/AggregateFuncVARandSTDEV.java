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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AggregateFuncVARandSTDEV {
    //    private static final String defaultConnectIP = "172.20.3.26";
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

    //创建var测试表1
    public void varTable1Create() throws SQLException {
        String table1Name = "vartest1";
        try(Statement statement = connection.createStatement()) {
            String createSQL1 = "create table " + table1Name +
                    " (id int,name varchar(32) not null,age int,amount double,address varchar(255),primary key(id))";

            statement.execute(createSQL1);
        }
    }

    //向var测试表1插入数据
    public void varTable1Insert(String varTable1Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable1SQL = "insert into vartest1 values " + varTable1Values;
            statement.execute(insertTable1SQL);
        }
    }

    //创建var测试表2
    public void varTable2Create() throws SQLException {
        String table2Name = "vartest2";
        try(Statement statement = connection.createStatement()) {
            String createSQL2 = "create table " + table2Name +
                    " (id int,name varchar(32) not null,age int,amount double,address varchar(255),primary key(id))";

            statement.execute(createSQL2);
        }
    }

    //向var测试表2插入数据
    public void varTable2Insert(String varTable2Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable2SQL = "insert into vartest2 values " + varTable2Values;
            statement.execute(insertTable2SQL);
        }
    }

    //创建var测试表3
    public void varTable3Create() throws SQLException {
        String table3Name = "vartest3";
        try(Statement statement = connection.createStatement()) {
            String createSQL3 = "create table " + table3Name +
                    " (id int,name varchar(32) not null,age int,primary key(id))";

            statement.execute(createSQL3);
        }
    }

    //向var测试表3插入数据
    public void varTable3Insert(String varTable3Values) throws SQLException {
        try(Statement statement = connection.createStatement();) {
            String insertTable3SQL = "insert into vartest3 values " + varTable3Values;
            statement.execute(insertTable3SQL);
        }
    }

    //创建var测试表4
    public void varTable4Create() throws SQLException {
        String table4Name = "vartest4";
        try(Statement statement = connection.createStatement()) {
            String createSQL4 = "create table " + table4Name +
                    " (id int,name varchar(32) not null,age int,primary key(id))";
            statement.execute(createSQL4);
        }
    }

    //向var测试表4插入数据
    public void varTable4Insert(String varTable4Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable4SQL = "insert into vartest4 values " + varTable4Values;
            statement.execute(insertTable4SQL);
        }
    }

    //创建var测试表5
    public void varTable5Create() throws SQLException {
        String table5Name = "vartest5";
        try(Statement statement = connection.createStatement()) {
            String createSQL5 = "create table " + table5Name + " (id int, name varchar(20), age int, amount double, " +
                    "address varchar(255), birthday date, create_time time, update_time timestamp, primary key (id))";

            statement.execute(createSQL5);
        }
    }

    //向var测试表5插入数据
    public void varTable5Insert(String varTable5Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable5SQL = "insert into vartest5 values " + varTable5Values;
            statement.execute(insertTable5SQL);
        }
    }

    //创建var测试表6-空表
    public void varTable6Create() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSQL6 = "create table product(id int, buyers int, price int, primary key(id))";
            statement.execute(createSQL6);
        }
    }

    //创建var测试表7
    public void varTable7Create() throws SQLException {
        String table7Name = "vartest7";
        try(Statement statement = connection.createStatement()) {
            String createSQL7 = "create table " + table7Name + " (id int,\n" +
                    "name varchar(32) not null,\n" +
                    "age int,amount double,\n" +
                    "address varchar(255),\n" +
                    "is_delete boolean,\n" +
                    "primary key(id))";

            statement.execute(createSQL7);
        }
    }

    //向var测试表7插入数据
    public void varTable7Insert(String varTable7Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable7SQL = "insert into vartest7 values " + varTable7Values;
            statement.execute(insertTable7SQL);
        }
    }

    //创建var测试表8
    public void varTable8Create() throws SQLException {
        String table8Name = "vartest8";
        try(Statement statement = connection.createStatement()) {
            String createSQL8 = "create table " + table8Name + " (id int,name varchar(32) not null," +
                    "age int default null,primary key(id))";

            statement.execute(createSQL8);
        }
    }

    //向var测试表8插入数据
    public void varTable8Insert(String varTable8Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertTable8SQL = "insert into vartest8 values " + varTable8Values;
            statement.execute(insertTable8SQL);
        }
    }

    //计算测试表1字段age的方差
    public Double varTable1Age() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarSQL1 = "select var(age) from vartest1";

            ResultSet ageVarRst = statement.executeQuery(ageVarSQL1);
            Double ageVarCal1 = null;
            while (ageVarRst.next()) {
                ageVarCal1 = ageVarRst.getDouble(1);
            }

            statement.close();
            return ageVarCal1;
        }
    }

    //计算测试表1 Double类型字段amount的方差
    public Double varTable1Amount() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String amountVarSQL1 = "select var(amount) from vartest1";

            ResultSet amountVarRst = statement.executeQuery(amountVarSQL1);
            Double amountVarCal1 = null;
            while (amountVarRst.next()) {
                amountVarCal1 = amountVarRst.getDouble(1);
            }

            statement.close();
            return amountVarCal1;
        }
    }

    //验证只有一条数据时返回null
    public double varTable1AgeOneRowCal() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarOneRowSQL1 = "select var(age) from vartest1 where id=1";

            ResultSet ageVarOneRowRst = statement.executeQuery(ageVarOneRowSQL1);
            Double ageVarOneRowCal = null;
            while (ageVarOneRowRst.next()) {
                ageVarOneRowCal = ageVarOneRowRst.getDouble(1);
            }

            statement.close();
            return ageVarOneRowCal;
        }
    }

    //计算测试表2字段age的方差
    public Double varTable2SkipNullAge() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarSQL2 = "select var(age) from vartest2";

            ResultSet ageVarRst = statement.executeQuery(ageVarSQL2);
            Double ageVarCal2 = null;
            while (ageVarRst.next()) {
                ageVarCal2 = ageVarRst.getDouble(1);
            }

            statement.close();
            return ageVarCal2;
        }
    }

    //计算测试表3字段age的方差,age值均相同,方差为0
    public Double varTableColumnSameValues() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarSQL3 = "select var(age) from vartest3";

            ResultSet ageVarRst = statement.executeQuery(ageVarSQL3);
            Double ageVarCal3 = null;
            while (ageVarRst.next()) {
                ageVarCal3 = ageVarRst.getDouble(1);
            }

            statement.close();
            return ageVarCal3;
        }
    }

    //计算测试表4字段age的方差,age值均为0，方差为0
    public Double varTableColumnZero() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarSQL4 = "select var(age) from vartest4";

            ResultSet ageVarRst = statement.executeQuery(ageVarSQL4);
            Double ageVarCal4 = null;
            while (ageVarRst.next()) {
                ageVarCal4 = ageVarRst.getDouble(1);
            }

            statement.close();
            return ageVarCal4;
        }
    }

    //计算多个字段的方差
    public List<Double> multiColumnVar() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String multiColumnVarSQL = "select VAR(id) vid,VAR(age) vag,VAR(amount) vam from vartest5";
            ResultSet multiColumnRST = statement.executeQuery(multiColumnVarSQL);
            List<Double> multiColumnVarList = new ArrayList<Double>();
            while(multiColumnRST.next()) {
                multiColumnVarList.add(multiColumnRST.getDouble("vid"));
                multiColumnVarList.add(multiColumnRST.getDouble("vag"));
                multiColumnVarList.add(multiColumnRST.getDouble("vam"));
            }

            statement.close();
            return multiColumnVarList;
        }
    }

    //空表6，计算方差返回null
    public Double varEmptyTableCal() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String emptyTableVarSQL = "select VAR(price) from product";

            ResultSet emptyTableRst = statement.executeQuery(emptyTableVarSQL);
            Double emptyTableVar = null;
            while (emptyTableRst.next()) {
                emptyTableVar = emptyTableRst.getDouble(1);
            }

            statement.close();
            return emptyTableVar;
        }
    }

    //和其他聚合函数一起使用
    public List varWithOtherAggrFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String varWithOtherAggrFuncSQL = "select avg(age),VAR(age),sum(amount),min(age),max(birthday)," +
                    "count(distinct(name)) from vartest5 where id in(1,3,5,7) and name<>'zhangsan'";
            ResultSet varWithOtherAggrRST = statement.executeQuery(varWithOtherAggrFuncSQL);
            List varWithOtherAggrList = new ArrayList();
            while(varWithOtherAggrRST.next()) {
                varWithOtherAggrList.add(varWithOtherAggrRST.getDouble(1));
                varWithOtherAggrList.add(varWithOtherAggrRST.getDouble(2));
                varWithOtherAggrList.add(varWithOtherAggrRST.getDouble(3));
                varWithOtherAggrList.add(varWithOtherAggrRST.getInt(4));
                varWithOtherAggrList.add(varWithOtherAggrRST.getDate(5));
                varWithOtherAggrList.add(varWithOtherAggrRST.getInt(6));

            }

            statement.close();
            return varWithOtherAggrList;
        }
    }

    //计算测试表5字段age的标准偏差
    public Double stdevTable5Age() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevSQL5 = "select stdev(age) from vartest5";

            ResultSet ageStdevRst = statement.executeQuery(ageStdevSQL5);
            Double ageStdevCal5 = null;
            while (ageStdevRst.next()) {
                ageStdevCal5 = ageStdevRst.getDouble(1);
            }

            statement.close();
            return ageStdevCal5;
        }
    }

    //计算测试表2字段age的标准偏差，跳过null
    public Double stdevTable2SkipNullAge() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevSQL2 = "select var(age) from vartest2";

            ResultSet ageStdevRst = statement.executeQuery(ageStdevSQL2);
            Double ageStdevCal2 = null;
            while (ageStdevRst.next()) {
                ageStdevCal2 = ageStdevRst.getDouble(1);
            }

            statement.close();
            return ageStdevCal2;
        }
    }

    //验证只有一条数据时返回null
    public double stdevTable5AgeOneRowCal() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevOneRowSQL5 = "select stdev(age) from vartest5 where id=1";

            ResultSet ageStdevOneRowRst = statement.executeQuery(ageStdevOneRowSQL5);
            Double ageStdevOneRowCal = null;
            while (ageStdevOneRowRst.next()) {
                ageStdevOneRowCal = ageStdevOneRowRst.getDouble(1);
            }

            statement.close();
            return ageStdevOneRowCal;
        }
    }

    //计算测试表3字段age的标准偏差,age值均相同,标准偏差为0
    public Double stdevTableColumnSameValues() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevSQL3 = "select stdev(age) from vartest3";

            ResultSet ageStdevRst = statement.executeQuery(ageStdevSQL3);
            Double ageStdevCal3 = null;
            while (ageStdevRst.next()) {
                ageStdevCal3 = ageStdevRst.getDouble(1);
            }

            statement.close();
            return ageStdevCal3;
        }
    }

    //计算测试表4字段age的标准偏差,age值均为0，标准偏差为0
    public Double stdevTableColumnZero() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevSQL4 = "select Stdev(age) from vartest4";

            ResultSet ageStdevRst = statement.executeQuery(ageStdevSQL4);
            Double ageStdevCal4 = null;
            while (ageStdevRst.next()) {
                ageStdevCal4 = ageStdevRst.getDouble(1);
            }

            statement.close();
            return ageStdevCal4;
        }
    }

    //计算测试表5 Double类型字段amount的标准偏差
    public Double stdevTable5Amount() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String amountStdevSQL5 = "select stdev(amount) from vartest5";

            ResultSet amountStdevRst = statement.executeQuery(amountStdevSQL5);
            Double amountStdevCal5 = null;
            while (amountStdevRst.next()) {
                amountStdevCal5 = amountStdevRst.getDouble(1);
            }

            statement.close();
            return amountStdevCal5;
        }
    }

    //计算多个字段的标准偏差
    public List<Double> multiColumnStdev() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String multiColumnStdevSQL = "select STDEV(id) sid,STDEV(age) sag,STDEV(amount) sam from vartest5";
            ResultSet multiColumnStdevRST = statement.executeQuery(multiColumnStdevSQL);
            List<Double> multiColumnStdevList = new ArrayList<Double>();
            while(multiColumnStdevRST.next()) {
                multiColumnStdevList.add(multiColumnStdevRST.getDouble("sid"));
                multiColumnStdevList.add(multiColumnStdevRST.getDouble("sag"));
                multiColumnStdevList.add(multiColumnStdevRST.getDouble("sam"));
            }

            statement.close();
            return multiColumnStdevList;
        }
    }

    //空表6，标准偏差返回null
    public Double stdevEmptyTableCal() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String emptyTableStdevSQL = "select stdev(price) from product";

            ResultSet emptyTableStdevRst = statement.executeQuery(emptyTableStdevSQL);
            Double emptyTableStdev = null;
            while (emptyTableStdevRst.next()) {
                emptyTableStdev = emptyTableStdevRst.getDouble(1);
            }

            statement.close();
            return emptyTableStdev;
        }
    }

    //和其他聚合函数一起使用
    public List stdevWithOtherAggrFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String stdevWithOtherAggrFuncSQL = "select avg(age),stdev(age),sum(amount),min(name)," +
                    "max(update_time),count(distinct(birthday)) from vartest5 where id<5";
            ResultSet stdevWithOtherAggrRST = statement.executeQuery(stdevWithOtherAggrFuncSQL);
            List stdevWithOtherAggrList = new ArrayList();
            while(stdevWithOtherAggrRST.next()) {
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getDouble(1));
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getDouble(2));
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getDouble(3));
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getString(4));
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getTimestamp(5));
                stdevWithOtherAggrList.add(stdevWithOtherAggrRST.getInt(6));

            }

            statement.close();
            return stdevWithOtherAggrList;
        }
    }

    //计算测试表8字段age的方差,返回null
    public Double varTable8Age() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageVarSQL8 = "select var(age) from vartest8";

            ResultSet ageVarRst8 = statement.executeQuery(ageVarSQL8);
            Double ageVarCal8 = null;
            while (ageVarRst8.next()) {
                ageVarCal8 = ageVarRst8.getDouble(1);
            }

            statement.close();
            return ageVarCal8;
        }
    }

    //计算测试表8字段age的标准偏差,返回null
    public Double stdevTable8Age() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ageStdevSQL8 = "select stdev(age) from vartest8";

            ResultSet ageStdevRst8 = statement.executeQuery(ageStdevSQL8);
            Double ageStdevCal8 = null;
            while (ageStdevRst8.next()) {
                ageStdevCal8 = ageStdevRst8.getDouble(1);
            }

            statement.close();
            return ageStdevCal8;
        }
    }
}
