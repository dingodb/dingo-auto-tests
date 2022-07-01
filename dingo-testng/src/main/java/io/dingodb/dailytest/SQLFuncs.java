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

public class SQLFuncs {
//    private static final String defaultConnectIP = "172.20.61.1";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";

    public static Connection connection;

    public static Connection connectDB() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(connectUrl);
        return connection;
    }

    public static String getFuncTableName() {
        final String funcTablePrefix = "funcTest";
        String funcTableName = funcTablePrefix + CommonArgs.getCurDateStr();
        return funcTableName;
    }

    public void createFuncTable() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String createFuncSQL = "create table " + funcTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "primary key(id)"
                + ")";
        statement.execute(createFuncSQL);
        statement.close();
//        connection.close();
    }

    public int insertMultiValues() throws ClassNotFoundException, SQLException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String batInsertSql = "insert into " + funcTableName +
                " values (1, 'Alice', 18, 3.5),\n" +
                "(2, 'Betty', 22, 4.1),\n" +
                "(3, 'Cindy', 39, 4.6),\n" +
                "(4, 'Doris', 25, 5.2),\n" +
                "(5, 'Emily', 24, 5.8),\n" +
                "(6, 'Alice', 32, 6.1),\n" +
                "(7, 'Betty', 18, 6.9),\n" +
                "(8, 'Alice', 22, 7.3),\n" +
                "(9, 'Cindy', 25, 3.5)";
        int insertMultiRowCount = statement.executeUpdate(batInsertSql);
        statement.close();
//        connection.close();
        return insertMultiRowCount;
    }

    //按姓名去重查询
    public List<String> distinctNameFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String distinctNameSql = "select distinct(name) dn from " + funcTableName;
        ResultSet distinctNameRst = statement.executeQuery(distinctNameSql);
        List<String> distinctNameList = new ArrayList<String>();
        while (distinctNameRst.next()) {
            distinctNameList.add(distinctNameRst.getString("dn"));
        }
        statement.close();
//        connection.close();
        return distinctNameList;
    }

    //按年龄去重查询
    public List<Integer> distinctAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String distinctAgeSql = "select distinct(age) from " + funcTableName;
        ResultSet distinctAgeRst = statement.executeQuery(distinctAgeSql);
        List<Integer> distinctAgeList = new ArrayList<Integer>();
        while (distinctAgeRst.next()) {
            distinctAgeList.add(distinctAgeRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return distinctAgeList;
    }

    //求平均
    public int avgAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String avgSql = "select avg(age) from " + funcTableName;
        ResultSet avgRst = statement.executeQuery(avgSql);
        int avgAge = 0;
        while (avgRst.next()) {
            avgAge = avgRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return avgAge;
    }

    //求和
    public int sumAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String sumSql = "select sum(age) from " + funcTableName;
        ResultSet sumRst = statement.executeQuery(sumSql);
        int sumAge = 0;
        while (sumRst.next()) {
            sumAge = sumRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return sumAge;
    }

    //求最大值
    public int maxAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String maxSql = "select max(age) from " + funcTableName;
        ResultSet maxRst = statement.executeQuery(maxSql);
        int maxAge = 0;
        while (maxRst.next()) {
            maxAge = maxRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return maxAge;
    }

    //求最小值
    public int minAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String minSql = "select min(age) from " + funcTableName;
        ResultSet minRst = statement.executeQuery(minSql);
        int minAge = 0;
        while (minRst.next()) {
            minAge = minRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return minAge;
    }

    //统计总数
    public int countFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String countSql = "select count(*) cnt from " + funcTableName;
        ResultSet countRst = statement.executeQuery(countSql);
        int countRows = 0;
        while (countRst.next()) {
            countRows = countRst.getInt("cnt");
        }
        statement.close();
//        connection.close();
        return countRows;
    }

    //升序
    public List<Integer> orderAscFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderAscSql = "select * from " + funcTableName + " order by age";
        ResultSet orderAscRst = statement.executeQuery(orderAscSql);
        List<Integer> orderAscAgeList = new ArrayList<Integer>();
        while (orderAscRst.next()) {
            orderAscAgeList.add(orderAscRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderAscAgeList;
    }

    //降序
    public List<Integer> orderDescFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderDescSql = "select * from " + funcTableName + " order by age desc";
        ResultSet orderDescRst = statement.executeQuery(orderDescSql);
        List<Integer> orderDescAgeList = new ArrayList<Integer>();
        while (orderDescRst.next()) {
            orderDescAgeList.add(orderDescRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderDescAgeList;
    }

    //不排序取limit
    public List<String> limitWithoutOrderAndOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String limitSql = "select * from " + funcTableName + " limit 5";
        ResultSet limitRst = statement.executeQuery(limitSql);
        List<String> limitList = new ArrayList<String>();
        while (limitRst.next()) {
            limitList.add(limitRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return limitList;
    }

    //排序取top1
    public List<Integer> orderLimitWithoutOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderLimitSql = "select * from " + funcTableName + " order by age desc limit 1";
        ResultSet orderLimitRst = statement.executeQuery(orderLimitSql);
        List<Integer> orderLimitList = new ArrayList<Integer>();
        while (orderLimitRst.next()) {
            orderLimitList.add(orderLimitRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderLimitList;
    }

    //排序指定偏移量取指定条数
    public List<Integer> orderLimitWithOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderLimitOffsetSql = "select * from " + funcTableName + " order by age desc limit 2 offset 3";
        ResultSet orderLimitOffsetRst = statement.executeQuery(orderLimitOffsetSql);
        List<Integer> orderLimitOffsetList = new ArrayList<Integer>();
        while (orderLimitOffsetRst.next()) {
            orderLimitOffsetList.add(orderLimitOffsetRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderLimitOffsetList;
    }

    //分组查询,获取sum值
    public List<Double> groupOrderAmountFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String groupOrderSql = "select name,sum(amount) sa from " + funcTableName + " group by name order by sa";
        ResultSet groupOrderRst = statement.executeQuery(groupOrderSql);
        List<Double> groupOrderAmountList = new ArrayList<Double>();
        while (groupOrderRst.next()) {
            groupOrderAmountList.add(groupOrderRst.getDouble("sa"));
        }
        statement.close();
//        connection.close();
        return groupOrderAmountList;
    }

    //分组查询，获取姓名列表
    public List<String> groupOrderNameFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String groupOrderSql = "select name,sum(amount) sa from " + funcTableName + " group by name order by sa";
        ResultSet groupOrderRst = statement.executeQuery(groupOrderSql);
        List<String> groupOrderNameList = new ArrayList<String>();
        while (groupOrderRst.next()) {
            groupOrderNameList.add(groupOrderRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return groupOrderNameList;
    }

    //按姓名删除
    public int deleteWithNameCondition() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String deleteNameSql = "delete from " + funcTableName + " where name=" + "'Alice" + "'";
        int deleteCount = statement.executeUpdate(deleteNameSql);
        statement.close();
//        connection.close();
        return deleteCount;
    }

    //按姓名条件删除后，获取姓名列表
    public List<String> getNameListAfterDeleteName() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String allSql = "select * from " + funcTableName;
        ResultSet allRst = statement.executeQuery(allSql);
        List<String> afterDeleteNameList = new ArrayList<String>();
        while (allRst.next()) {
            afterDeleteNameList.add(allRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return afterDeleteNameList;
    }

    //验证cast函数，将字符串转化为整形
    public int castFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String insertSql = "insert into " + funcTableName + " values (10,'10',15,24.2)";
        statement.execute(insertSql);

        String castSql = "select cast(name as int) + 123 as castnum from " + funcTableName + " where name='10'";
        ResultSet castNameRst = statement.executeQuery(castSql);
        int afterCast = 0;
        while (castNameRst.next()) {
            afterCast += castNameRst.getInt("castnum");
        }
        statement.close();
//        connection.close();
        return afterCast;
    }



    /**
    *  v0.2.0版本补充测试用例
    */

    //创建测试表格
    public void createEmpTable() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();

        String createTableSQL = "create table emptest065(id int not null, name varchar(20), " +
                "age int, amount double, address varchar(255), primary key (id))";

        statement.execute(createTableSQL);
        statement.close();
    }

    //表为空时，求最小值
    public String case065() throws SQLException, ClassNotFoundException {
        createEmpTable();
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select min(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String minAge = null;
        while(resultSet.next()) {
            minAge = resultSet.getString(1);
        }
        statement.close();
        return minAge;
    }

    //表为空时，求最大值
    public String case069() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select max(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String maxAge = null;
        while(resultSet.next()) {
            maxAge = resultSet.getString(1);
        }
        statement.close();
        return maxAge;
    }

    //表为空时求和
    public String case073() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select sum(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String sumAge = null;
        while(resultSet.next()) {
            sumAge = resultSet.getString(1);
        }
        statement.close();
        return sumAge;
    }

    //表为空时，求平均
    public String case259() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select avg(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String avgAge = null;
        while(resultSet.next()) {
            avgAge = resultSet.getString(1);
        }
        statement.close();
        return avgAge;
    }


    //表为空时，统计总数
    public int case074() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select count(*) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int countNum = 0;

        while(resultSet.next()) {
            countNum = resultSet.getInt(1);
        }
        statement.close();
        return countNum;
    }

    //表为空时，升序排序
    public boolean case263() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age asc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;

    }

    //表为空时，降序排序
    public boolean case272() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;

    }

    //表为空时，分组查询
    public boolean case281() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,sum(amount) sa from emptest065 group by name";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }


    //插入单行数据
    public void insertOneRowToTable() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values(1,'zhangsan',18,23.50,'Beijing')";
        statement.execute(insertSQL);
        statement.close();
    }

    //单行数据求最小值
    public int case066() throws SQLException, ClassNotFoundException {
        insertOneRowToTable();
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select min(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int minAge = 0;
        while(resultSet.next()) {
            minAge = resultSet.getInt(1);
        }
        statement.close();
        return minAge;
    }

    //单行数据求最大值
    public int case070() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select max(age) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int maxAge = 0;
        while(resultSet.next()) {
            maxAge = resultSet.getInt(1);
        }
        statement.close();
        return maxAge;
    }

    //单行数据升序排序
    public List case262() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List orderAgeList = new ArrayList();
        while(resultSet.next()) {
            orderAgeList.add(resultSet.getInt("age"));
        }
        statement.close();
        return orderAgeList;
    }

    //单行数据降序排序
    public List case271() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List orderAgeList = new ArrayList();
        while(resultSet.next()) {
            orderAgeList.add(resultSet.getInt("age"));
        }
        statement.close();
        return orderAgeList;
    }

    //插入多行
    public void insertMoreRowsToTable() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values \n" +
                "(2,'lisi',25,895,' beijing haidian '),\n" +
                "(3,'l3',55,123.123,'wuhan NO.1 Street'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(5,'awJDs',1,1453.9999,'pingYang1'),\n" +
                "(6,'123',544,0,'543'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang'),\n" +
                "(8,'zhangsan',18,12.3,'shanghai'),\n" +
                "(9,'op ',76,109.325,'wuhan'),\n" +
                "(10,'lisi',256,1234.456,'nanjing'),\n" +
                "(11,'  aB c  dE ',61,99.9999,'beijing chaoyang'),\n" +
                "(12,' abcdef',2,2345.000,'123'),\n" +
                "(13,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(14,'zhngsna',99,32,'chong qing '),\n" +
                "(15,'1.5',18,0.1235,'http://WWW.baidu.com')";
        statement.execute(insertSQL);
        statement.close();
    }

    //字符串类型字段求最小值
    public String case067() throws SQLException, ClassNotFoundException {
        insertMoreRowsToTable();
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select min(name) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String minNameStr = null;
        while(resultSet.next()) {
            minNameStr = resultSet.getString(1);
        }
        statement.close();
        return minNameStr;
    }

    //字符串类型字段求最大值
    public String case071() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select max(name) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        String maxNameStr = null;
        while(resultSet.next()) {
            maxNameStr = resultSet.getString(1);
        }
        statement.close();
        return maxNameStr;
    }

    //double类型字段求最小值
    public Double case068() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select min(amount) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Double minAmount = null;
        while(resultSet.next()) {
            minAmount = resultSet.getDouble(1);
        }
        statement.close();
        return minAmount;
    }

    //double类型字段求最大值
    public Double case072() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select max(amount) from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Double maxAmount = null;
        while(resultSet.next()) {
            maxAmount = resultSet.getDouble(1);
        }
        statement.close();
        return maxAmount;
    }

    //double型转整型
    public List case136() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select cast(amount as int) as canum from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List amountList = new ArrayList();
        while (resultSet.next()) {
            amountList.add(resultSet.getInt("canum"));
        }
        statement.close();
        return amountList;
    }

    //整型转double型
    public List case137() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select cast(age as double) as cad from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List ageList = new ArrayList();
        while (resultSet.next()) {
            ageList.add(resultSet.getDouble("cad"));
        }

        statement.close();
        return ageList;
    }

    //对varchar类型字段求和
    public void case257() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select sum(name) from emptest065";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //varchar类型值均为数值时，求和
    public void case258() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select sum(name) from emptest065 where id=6 or id=15";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //对varchar类型字段求平均
    public void case260() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select avg(name) from emptest065";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //varchar类型值均为数值时，求平均
    public void case261() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select avg(name) from emptest065 where id=6 or id=15";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //根据字符型字段升序
    public List<List> case265() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,name from emptest065 order by name";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //根据字符型字段降序
    public List<List> case273() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,name from emptest065 order by name desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //按两个字段升序
    public List<List> case266() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,name,age,amount from emptest065 order by age,amount";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //按两个字段降序
    public List<List> case274() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by age desc,amount desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }


    //按三个字段升序
    public List<List> case267() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by age,name,amount";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //按三个字段降序
    public List<List> case275() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by age desc,name desc,amount desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //缺少排序字段
    public void case268() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //缺少排序字段
    public void case276() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by DESC";
        statement.executeQuery(querySQL);
        statement.close();
    }


    //排序字段重复 - 升序
    public List case269() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age,age";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List queryList = new ArrayList<>();

        while(resultSet.next()) {
            queryList.add(resultSet.getInt(1));
        }
        statement.close();
        return queryList;
    }

    //排序字段重复 - 降序
    public List case277() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select age from emptest065 order by age desc,age desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List queryList = new ArrayList<>();

        while(resultSet.next()) {
            queryList.add(resultSet.getInt(1));
        }
        statement.close();
        return queryList;
    }

    //升降序同时使用
    public List<List> case279_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by age desc,amount";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //升降序同时使用
    public List<List> case279_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 order by name,age desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //升序降序同时使用，字段相同
    public List<List> case280_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,age from emptest065 order by age desc,age";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //升序降序同时使用，字段相同
    public List<List> case280_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,age from emptest065 order by age,age desc";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //使用聚合字段分组，预期失败
    public void case282() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,sum(amount) sa from emptest065 group by sa";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //使用非查询字段分组
    public List<List> case283() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select sum(age) sage, sum(amount) samount from emptest065 group by name";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt("sage")));
            rowList.add(String.valueOf(resultSet.getDouble("samount")));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //全表分组，预期失败
    public void case284() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 group by age";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //使用多字段分组
    public List<List> case285_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age,sum(amount) from emptest065 group by name,age";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            rowList.add(String.valueOf(resultSet.getDouble(3)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //使用多字段分组
    public List<List> case285_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age,amount,address from emptest065 group by name,age,amount,address";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            rowList.add(String.valueOf(resultSet.getDouble(3)));
            rowList.add(resultSet.getString(4));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    public int case286() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 limit 20";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int rowNum = 0;

        while(resultSet.next()) {
            rowNum++;
        }
        statement.close();
        return rowNum;
    }

    public int case289_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 5 offset 0";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int rowNum = 0;

        while(resultSet.next()) {
            rowNum++;
        }
        statement.close();
        return rowNum;
    }

    public int case289_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 3 offset 14";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int rowNum = 0;

        while(resultSet.next()) {
            rowNum++;
        }
        statement.close();
        return rowNum;
    }

    public int case289_3() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 20 offset 1";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int rowNum = 0;

        while(resultSet.next()) {
            rowNum++;
        }
        statement.close();
        return rowNum;
    }

    public boolean case290() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 5 offset 15";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //限制输出条数为0
    public boolean case291_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 0";
        ResultSet resultSet = statement.executeQuery(querySQL);

        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //限制输出条数为0
    public boolean case291_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 0 offset 3";
        ResultSet resultSet = statement.executeQuery(querySQL);

        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //限制输出条数为负数
    public void case292_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit -1";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //限制输出条数为负数
    public void case292_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit -1 offset 3";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //限制输出条数为小数
    public List<List> case293_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 3.5";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //限制输出条数为小数
    public List<List> case293_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 limit 3.5 offset 3";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //限制输出条数为字符串
    public void case294_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit '2'";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //限制输出条数为字符串
    public void case294_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit '3' offset 3";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //offset为负数
    public void case295_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 3 offset -2";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //offset为字符串
    public void case295_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 3 offset '3'";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //限制起始索引为小数
    public List<List> case295_3() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 3 offset 3.5";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //参数缺失，预期异常
    public void case296_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit offset 3";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //参数缺失，预期异常
    public void case296_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit 5 offset";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //参数缺失，预期异常
    public void case296_3() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 limit";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //in运算符，指定范围只一个元素
    public List<List> case297() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name in ('zhangsan')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符，指定范围只两个元素
    public List<List> case298() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name in ('zhangsan','lisi')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符，指定范围有元素不在表里
    public List<List> case299_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name in ('zhangsan','haha','lisi')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符，指定范围有元素不在表里
    public List<List> case299_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 where id in (1,3,5,7,9,11,13,15,17)";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符，指定元素全都不在表里
    public boolean case300() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,age from emptest065 where id in (16,18,20)";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //in运算符，多个字段
    public List<List> case301() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id in (1,3,5,7,9,10,13) " +
                "and name in ('zhangsan','lisi')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //创建表，插入含有空字符串的值
    public void createTable302() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createTableSql = "create table test302(id int not null, name varchar(20), " +
                "age int, amount double, address varchar(255), primary key (id))";
        statement.execute(createTableSql);

        String insertValueSql = "insert into test302 values \n" +
                "(1,'zhangsan',18,90.33,'beijing'),\n" +
                "(2,'lisi',35,120.98,''),\n" +
                "(3,'Hello',35,18.0,'beijing'),\n" +
                "(4,'HELLO2',15,23.0,'chaoyangdis_1 NO.street'),\n" +
                "(5,'lala',18,12.1234560987,'beijing'),\n" +
                "(6,'88',18,12.0,'changping 89'),\n" +
                "(7,'baba',99,23.51648,''),\n" +
                "(8,'zala',100,54.0,'wuwuxi '),\n" +
                "(9,' uzlia ',28,23.6,'  maya'),\n" +
                "(10,'  MaiTeng',66,70.3,'ding TAO  '),\n" +
                "(11,'',0,0.01,'')";
        statement.execute(insertValueSql);
        statement.close();
    }

    //in运算符,范围为空字符串
    public List<List> case302() throws SQLException, ClassNotFoundException {
        createTable302();
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from test302 where address in ('')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符，指定范围只一个元素
    public List<List> case308() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name not in ('zhangsan')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符，指定范围有多个元素
    public List<List> case309_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name not in ('zhangsan','lisi')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符，指定范围有多个元素
    public List<List> case309_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id not in (1,3,5,7,9,10,15)";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符，指定范围不在表中
    public List<List> case310() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name not in ('test')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符，多个字段
    public List<List> case311() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id not in (1,3,5,7,9,10) and name not in ('zhangsan','lisi')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符,范围为空字符串
    public List<List> case312() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from test302 where address not in ('')";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符,update语句中使用
    public List<List> case306() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String updateSQL = "update test302 set name='laozhang' where name in ('zhangsan','lisi','88')";
        statement.executeUpdate(updateSQL);
        String querySQL = "select id,name from test302 where name='laozhang'";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符,update语句中使用
    public List<List> case313() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String updateSQL = "update test302 set address='BJ' where address not in ('beijing','') and id in (9,10,11)";
        statement.executeUpdate(updateSQL);
        String querySQL = "select * from test302";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //in运算符,delete语句中使用
    public List<List> case307() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String deleteSQL = "delete from test302 where name in ('laozhang','lala','baba')";
        statement.executeUpdate(deleteSQL);
        String querySQL = "select * from test302";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //not in运算符,delete语句中使用
    public List<List> case314() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String deleteSQL = "delete from test302 where age not in(15,28,0) and address='BJ'";
        statement.executeUpdate(deleteSQL);
        String querySQL = "select * from test302";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }


    //单个and使用
    public List<List> case315() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name='zhangsan' and age=18";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个and使用
    public List<List> case316_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,name from emptest065 where id>5 and name='zhangsan' and age=18";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个and使用
    public List<List> case316_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id<10 and name not in('lisi') and age<60 and amount>20.0";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个and使用
    public List<List> case316_3() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id in(2,4,6,8,10) and age>=18 and name='zhangsan'";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //单个or使用
    public List<List> case319() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where name='zhangsan' or age=18";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个or使用
    public List<List> case320_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name from emptest065 where address='123' or name='zhangsan' or age=18";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个or使用
    public List<List> case320_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name from emptest065 where id<5 or address<>'Beijing' " +
                "or name in ('zhangsan','lisi') or age not in (18,60)";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //and和or一起使用
    public List<List> case323() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where id>5 and name='zhangsan' or age=18 and address='Beijing'";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //查询支持括号
    public List<List> case324() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select * from emptest065 where (id>5 and name='zhangsan' or age=18) and address='Beijing'";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //所有字段去重
    public List<List> case325() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select distinct * from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //多个字段去重
    public List<List> case326() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select distinct name,age,amount,address from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(String.valueOf(resultSet.getInt(2)));
            rowList.add(String.valueOf(resultSet.getDouble(3)));
            rowList.add(resultSet.getString(4));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //distinct关键字不在所有字段前
    public void case327_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,distinct name,age from emptest065";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //distinct关键字不在所有字段前
    public void case327_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,distinct(name) from emptest065";
        statement.executeQuery(querySQL);
        statement.close();
    }

    //count和distinct一起使用
    public int case329() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select count(distinct(name)) cdn from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);
        int countNum = 0;
        while(resultSet.next()) {
            countNum = resultSet.getInt("cdn");
        }
        statement.close();
        return countNum;
    }

    //distinct在空表中使用
    public boolean case330() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createTable = "create table case330(id int, name varchar(20), primary key(id))";
        statement.execute(createTable);
        String querySQL = "select distinct(id) from case330";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //无重复
    public List<List> case331() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select distinct(name) from test302";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));

            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //复合查询1
    public List<List> case332() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select name,sum(amount) sa from emptest065 where id>3 and age<60 " +
                "group by name order by sa DESC limit 5 offset 3";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString("name"));
            rowList.add(String.valueOf(resultSet.getDouble("sa")));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //复合查询2
    public List<List> case333() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select avg(age) aa,min(amount) ma,address from emptest065 where id in(1,3,5,7,9,11,13,15) " +
                "or name<>'zhangsan' group by address order by min(amount) limit 2";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt("aa")));
            rowList.add(String.valueOf(resultSet.getDouble("ma")));
            rowList.add(resultSet.getString("address"));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //复合查询3
    public List case334() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select count(distinct(name)),count(distinct(age)),count(distinct(address))," +
                "format(sum(amount),3),avg(age),max(id),min(amount) from emptest065 where age>18";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List queryList = new ArrayList();
        while(resultSet.next()) {
            queryList.add(resultSet.getString(1));
            queryList.add(resultSet.getString(2));
            queryList.add(resultSet.getString(3));
            queryList.add(resultSet.getString(4));
            queryList.add(resultSet.getString(5));
            queryList.add(resultSet.getString(6));
            queryList.add(resultSet.getString(7));
        }

        statement.close();
        return queryList;
    }

    //插入重复主键
    public int case335() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values(1,'insert1',19,1.0,'shanghai'),(2,'insert2',29,20,'Beijing')";
        int effectNum = statement.executeUpdate(insertSQL);
        statement.close();
        return effectNum;
    }

    //插入数据和字段不符，预期异常
    public void case336_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values(20,'zhang',18)";
        statement.executeUpdate(insertSQL);
        statement.close();
    }

    //插入数据和字段不符，预期异常
    public void case336_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values(21,'haha',18,19.0,'addr',2020)";
        statement.executeUpdate(insertSQL);
        statement.close();
    }

    //插入数据和字段类型不符，预期异常
    public void case337() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into emptest065 values(22,'haha','abc',19.0,'beijing')";
        statement.executeUpdate(insertSQL);
        statement.close();
    }

    //修改表数据，要修改的值不存在
    public int case338() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String updateSQL = "update emptest065 set name='update1' where name='1234'";
        int effectNum = statement.executeUpdate(updateSQL);
        statement.close();
        return effectNum;
    }

    //表为空时，删除表数据
    public int case339() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String updateSQL = "delete from case330";
        int effectNum = statement.executeUpdate(updateSQL);
        statement.close();
        return effectNum;
    }

    //删除结果集为空
    public int case340() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String deleteSQL = "delete from emptest065 where name='zhangsan' and name='lisi'";
        int deletenum = statement.executeUpdate(deleteSQL);
        statement.close();
        return deletenum;
    }

    //删除表不存在
    public void case341() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String dropSQL = "drop table case341";
        statement.execute(dropSQL);
        statement.close();
    }

    //验证删除表，表数据一起删除
    public boolean case342() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createTable = "create table case342(id int, name varchar(20), primary key(id))";
        statement.execute(createTable);
        String insertSQL = "insert into case342 values(1,'case342-1'),(2,'case342-2')";
        statement.executeUpdate(insertSQL);
        String dropSQL = "drop table case342";
        statement.execute(dropSQL);
        statement.execute(createTable);
        String querySQL = "select * from case342";
        ResultSet resultSet = statement.executeQuery(querySQL);
        Boolean queryResult = resultSet.next();
        statement.close();
        return queryResult;
    }

    //未指定not null
    public List<List> case343() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case343(id int,name varchar(32) not null,age int," +
                "amount double,address varchar(255),primary key(id))";
        statement.execute(createSQL);
        String insertSQL = "insert into case343(id,name) values(1,'hello')";
        statement.executeUpdate(insertSQL);

        String querySQL = "select * from case343";
        ResultSet resultSet = statement.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            rowList.add(resultSet.getString(4));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }

        statement.close();
        return queryList;
    }

    //指定Null,指定默认值
    public List<List> case344() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case344(id int,name varchar(32) not null,age int null default 20," +
                "amount double,address varchar(255),primary key(id))";
        statement.execute(createSQL);
        String insertSQL = "insert into case344(id,name) values(1,'hello')";
        statement.executeUpdate(insertSQL);

        String querySQL = "select * from case344";
        ResultSet resultSet = statement.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(resultSet.getString(4));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //指定not Null,指定默认值
    public List<List> case624() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case624(id int,name varchar(32) not null,age int not null default 20," +
                "amount double,address varchar(255) default 'shanghai',primary key(id))";
        statement.execute(createSQL);
        String insertSQL = "insert into case624(id,name) values(1,'hello')";
        statement.executeUpdate(insertSQL);

        String querySQL = "select * from case624";
        ResultSet resultSet = statement.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(resultSet.getString(4));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }



    //删除记录，再插入相同主键的记录可成功
    public int case346() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String deleteSQL = "delete from emptest065 where id=2";
        statement.execute(deleteSQL);
        String insertSQL = "insert into emptest065 values(2,'lisi',25,895,' beijing haidian ')";
        int updateNum = statement.executeUpdate(insertSQL);

        statement.close();
        return updateNum;
    }

    //通过select语句插入数据
    public List<List> case1049() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case1049(id int,name varchar(32) not null,age int," +
                "amount double,address varchar(255),primary key(id))";
        statement.execute(createSQL);
        String insertSQL = "insert into case1049 select * from emptest065 where id>5";
        statement.executeUpdate(insertSQL);

        String querySQL = "select * from case1049";
        ResultSet resultSet = statement.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(String.valueOf(resultSet.getInt(1)));
            rowList.add(resultSet.getString(2));
            rowList.add(String.valueOf(resultSet.getInt(3)));
            rowList.add(String.valueOf(resultSet.getDouble(4)));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //count函数，不计入null
    public List case1119() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case1119(id int, name varchar(20), age int, primary key(id))";
        statement.execute(createSQL);
        String insertSQL1 = "insert into case1119(id,name) values (1,'zhangsan'),(2,'hello')";
        String insertSQL2 = "insert into case1119 values (3,'wangwu',18),(4,'Kla',20)";
        statement.executeUpdate(insertSQL1);
        statement.executeUpdate(insertSQL2);

        String querySQL1 = "select count(age) from case1119";
        ResultSet resultSet1 = statement.executeQuery(querySQL1);

        List queryList = new ArrayList();
        while(resultSet1.next()) {
            queryList.add(String.valueOf(resultSet1.getInt(1)));
        }

        String querySQL2 = "select count(1) from case1119";
        ResultSet resultSet2 = statement.executeQuery(querySQL2);
        while(resultSet2.next()) {
            queryList.add(String.valueOf(resultSet2.getInt(1)));
        }

        String querySQL3 = "select count(*) from case1119";
        ResultSet resultSet3 = statement.executeQuery(querySQL3);
        while(resultSet3.next()) {
            queryList.add(String.valueOf(resultSet3.getInt(1)));
        }
        statement.close();
        return queryList;
    }

    //or在update语句中使用
    public List<List> case321() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement1 = connection.createStatement();
        String updateSQL = "update emptest065 set name='new' where name='zhangsan' or age=18";
        statement1.execute(updateSQL);

        Statement statement2 = connection.createStatement();
        String querySQL = "select id,name from emptest065 where name='new'";
        ResultSet resultSet2 = statement2.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet2.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet2.getString(1));
            rowList.add(resultSet2.getString(2));
            queryList.add(rowList);
        }

        statement1.close();
        statement2.close();
        return queryList;
    }

    //or在update语句中使用
    public List<List> case322() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String deleteSQL = "delete from emptest065 where age<18 or age>60 or name='new'";
        statement.execute(deleteSQL);
        String querySQL = "select * from emptest065";
        ResultSet resultSet = statement.executeQuery(querySQL);

        List<List> queryList = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            rowList.add(resultSet.getString(4));
            rowList.add(resultSet.getString(5));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    //非主键插入null值，预期成功
    public List case1443_1() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case1443(id int, name varchar(20), " +
                "age int, amount double, address varchar(255),birthday date, " +
                "create_time time, update_time timestamp, is_delete boolean,primary key(id))";
        statement.execute(createSQL);
        String insertSQL = "insert into case1443 values (1,null,null,null,null,null,null,null,null)";
        statement.executeUpdate(insertSQL);
        String querySQL = "select * from case1443";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List actualRecord = new ArrayList();
        while(resultSet.next()) {
            actualRecord.add(resultSet.getString(1));
            actualRecord.add(resultSet.getString(2));
            actualRecord.add(resultSet.getString(3));
            actualRecord.add(resultSet.getString(4));
            actualRecord.add(resultSet.getString(5));
            actualRecord.add(resultSet.getString(6));
            actualRecord.add(resultSet.getString(7));
            actualRecord.add(resultSet.getString(8));
            actualRecord.add(resultSet.getString(9));
        }
        statement.close();
        return actualRecord;
    }

    //主键插入null值，预期失败
    public void case1443_2() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into case1443(null,null,null,null,null,null,null,null,null)";
        statement.executeUpdate(insertSQL);
        statement.close();
    }

    //创建宽表
    public void createWidthTable(String tableMeta) throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String createSQL = "create table case1483" + tableMeta;
        statement.execute(createSQL);
        statement.close();
    }

    //插入数据到宽表
    public int insertWidthTable(String tableValues) throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String insertSQL = "insert into case1483 values " + tableValues;
        int insertRows = statement.executeUpdate(insertSQL);
        statement.close();
        return insertRows;
    }

    public List<List> searchWidthTable() throws SQLException, ClassNotFoundException {
        connection = connectDB();
        Statement statement = connection.createStatement();
        String querySQL = "select id,name,uuid,write_date,is_delete from case1483 where data_old and not is_delete";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> actualRecord = new ArrayList<List>();
        while(resultSet.next()) {
            List rowList = new ArrayList();
            rowList.add(resultSet.getString(1));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            rowList.add(resultSet.getDate(4).toString());
            rowList.add(resultSet.getBoolean(5));
            actualRecord.add(rowList);
        }
        statement.close();
        return actualRecord;
    }


}
