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

public class BooleanField {
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

    //生成的测试表格名称并返回
    public static String getBooleanTableName() {
        final String booleanTablePrefix = "booleanFieldTest";
        String booleanTableName = booleanTablePrefix + CommonArgs.getCurDateStr();
        return booleanTableName;
    }

    //创建表格
    public void createBooleanTable() throws SQLException, ClassNotFoundException {
        String strBooleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement();) {
            String createBLTableSQL = "create table " + strBooleanTableName + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "address varchar(255),"
                    + "is_delete boolean,"
                    + "primary key(id)"
                    + ")";
            statement.execute(createBLTableSQL);
        }
    }

    //插入true数据
    public int insertTrueValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String insertTrueSql = "insert into " + booleanTableName + " values "
                    + "(1,'zhangsan',18,23.50,'beijing', true),\n"
                    + "(2,'lisi',25,89,' beijinghaidian ', true),\n"
                    + "(3,'lisi3',55,123.123,'wuhan NO.1 Street', TRUE),\n"
                    + "(4,'HAHA',57,9.0762556,'CHANGping', True)";

            int insertTrueRows = statement.executeUpdate(insertTrueSql);
            statement.close();
            return insertTrueRows;
        }
    }

    // 查看true数据
    public List<Boolean> getTrueValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryTrueSql = "select * from " + booleanTableName + " where id < 5";
            ResultSet queryTrueRst = statement.executeQuery(queryTrueSql);
            List<Boolean> trueList = new ArrayList<Boolean>();
            while (queryTrueRst.next()) {
                trueList.add(queryTrueRst.getBoolean("is_delete"));
            }

            statement.close();
            return trueList;
        }
    }

    //插入false数据
    public int insertFalseValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String insertFalseSql = "insert into " + booleanTableName + " values "
                    + "(5,'wJDs',1,1453.9999,'pingYang1', FALSE),\n"
                    + "(6,'123',544,0,'543', false),\n"
                    + "(7,'yamaha',76,2.30,'beijing changyang', False),\n"
                    + "(8,'zhangsan',18,12.3,'shanghai', false)";

            int insertFalseRows = statement.executeUpdate(insertFalseSql);
            statement.close();
            return insertFalseRows;
        }
    }

    // 查看false数据
    public List<Boolean> getFalseValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryFalseSql = "select * from " + booleanTableName + " where id >4 and id < 9";
            ResultSet queryFalseRst = statement.executeQuery(queryFalseSql);
            List<Boolean> falseList = new ArrayList<Boolean>();
            while (queryFalseRst.next()) {
                falseList.add(queryFalseRst.getBoolean("is_delete"));
            }

            statement.close();
            return falseList;
        }
    }

    //插入true和false数据
    public int insertTrueAndFalseValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String insertTrueAndFalseSql = "insert into " + booleanTableName + " values "
                    + "(9,'oppo',76,109.325,'wuhan', True),\n"
                    + "(10,'lisi',256,1234.456,'nanjing', False),\n"
                    + "(11,' ab c d ',61,99.9999,'beijing chaoyang', true),\n"
                    + "(12,' abcdef',2,2345.000,'123', false),\n"
                    + "(13,'HX K', 99, 22.22, 'Haidian Distinct', False),\n"
                    + "(14,'YH', 76, 1.01, 'TJ Street No.1', True),\n"
                    + "(15, 'yamaha',76,2.30,'beijing changyang', TRUE)";

            int insertTrueAndFalseRows = statement.executeUpdate(insertTrueAndFalseSql);
            statement.close();
            return insertTrueAndFalseRows;
        }
    }

    // 查看true和false数据
    public List<Boolean> getTrueAndFalseValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryTrueAndFalseSql = "select * from " + booleanTableName + " where id > 8 and id < 16";
            ResultSet queryTrueAndFalseRst = statement.executeQuery(queryTrueAndFalseSql);
            List<Boolean> trueAndfalseList = new ArrayList<Boolean>();
            while (queryTrueAndFalseRst.next()) {
                trueAndfalseList.add(queryTrueAndFalseRst.getBoolean("is_delete"));
            }
            statement.close();
            return trueAndfalseList;
        }
    }

    // 查看按true值条件查询数据
    public List<String> queryTrueValue() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryTrueValueSql = "select * from " + booleanTableName + " where is_delete = true";
            ResultSet queryTrueValueRst = statement.executeQuery(queryTrueValueSql);
            List<String> queryTrueValueList = new ArrayList<String>();
            while (queryTrueValueRst.next()) {
                queryTrueValueList.add(queryTrueValueRst.getString("NAME"));
            }
            statement.close();
            return queryTrueValueList;
        }
    }

    // 查看按false值条件查询数据
    public List<Integer> queryFalseValue() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryFalseValueSql = "select * from " + booleanTableName + " where is_delete = false";
            ResultSet queryFalseValueRst = statement.executeQuery(queryFalseValueSql);
            List<Integer> queryFalseValueList = new ArrayList<Integer>();
            while (queryFalseValueRst.next()) {
                queryFalseValueList.add(queryFalseValueRst.getInt(1));
            }
            statement.close();
            return queryFalseValueList;
        }
    }

    // 字段为真条件查询数据
    public List<Boolean> fieldAsConditionValue() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String fieldAsConditionSql = "select * from " + booleanTableName + " where is_delete";
            ResultSet fieldAsConditionRst = statement.executeQuery(fieldAsConditionSql);
            List<Boolean> fieldConditionList = new ArrayList<Boolean>();
            while (fieldAsConditionRst.next()) {
                fieldConditionList.add(fieldAsConditionRst.getBoolean("is_delete"));
            }
            statement.close();
            return fieldConditionList;
        }
    }

    // not字段为假条件查询数据
    public List<Boolean> notFieldAsConditionValue() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String notFieldAsConditionSql = "select * from " + booleanTableName + " where not is_delete";
            ResultSet notFieldAsConditionRst = statement.executeQuery(notFieldAsConditionSql);
            List<Boolean> notFieldConditionList = new ArrayList<Boolean>();
            while (notFieldAsConditionRst.next()) {
                notFieldConditionList.add(notFieldAsConditionRst.getBoolean("is_delete"));
            }
            statement.close();
            return notFieldConditionList;
        }
    }

    // is true 查询
    public List<String> queryIsTrue() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select * from " + booleanTableName + " where is_delete is true";
            ResultSet queryRst = statement.executeQuery(querySql);
            List<String> queryIsTrueList = new ArrayList<String>();
            while (queryRst.next()) {
                queryIsTrueList.add(queryRst.getString("NAME"));
            }
            statement.close();
            return queryIsTrueList;
        }
    }

    // is false 查询
    public List<Integer> queryIsFalse() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select * from " + booleanTableName + " where is_delete is false";
            ResultSet queryRst = statement.executeQuery(querySql);
            List<Integer> queryIsFalseList = new ArrayList<Integer>();
            while (queryRst.next()) {
                queryIsFalseList.add(queryRst.getInt(1));
            }
            statement.close();
            return queryIsFalseList;
        }
    }

    //插入0转换为false
    public int insertZeroValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String insertZeroSql = "insert into " + booleanTableName + " values "
                    + "(35,'oppo',20,1.2,'wuhan', 0)";

            int insertZeroRows = statement.executeUpdate(insertZeroSql);
            statement.close();
            return insertZeroRows;
        }
    }

    // 查看0转换后数据
    public Boolean getZeroValues() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String queryZeroSql = "select * from " + booleanTableName + " where id=35";
            ResultSet queryZeroRst = statement.executeQuery(queryZeroSql);
            Boolean zeroOut = null;
            while (queryZeroRst.next()) {
                zeroOut = queryZeroRst.getBoolean("is_delete");
            }
            statement.close();
            return zeroOut;
        }
    }

    //插入正整数转换为true
    public int insertPosIntegerValues(String insertID, String insertIntegerValue) throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String insertPosIntegerSql = "insert into " + booleanTableName + " values "
                    + "(" + insertID + ",'oppo',20,1.2,'wuhan'," + insertIntegerValue + ")";

            int insertIntegerRows = statement.executeUpdate(insertPosIntegerSql);
            statement.close();
            return insertIntegerRows;
        }
    }

    // 查看正整数转换后数据
    public Boolean getIntegerValues(String insertID) throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()){
            String queryIntegerSql = "select * from " + booleanTableName + " where id = " + insertID;
            ResultSet queryIntegerRst = statement.executeQuery(queryIntegerSql);
            Boolean integerOut = null;
            while (queryIntegerRst.next()) {
                integerOut = queryIntegerRst.getBoolean("is_delete");
            }
            statement.close();
            return integerOut;
        }
    }

    //布尔字段插入null值
    public String insertNull() throws SQLException {
        String booleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()){
            String insertSql = "insert into " + booleanTableName + " values(10001, 'kelay',76,2.30,'shanghai', null)";
            int insertNum = statement.executeUpdate(insertSql);
            String querySql = "select * from " + booleanTableName + " where id=10001";
            ResultSet resultSet = statement.executeQuery(querySql);
            String integerOut = null;
            while (resultSet.next()) {
                integerOut = resultSet.getString("is_delete");
            }
            statement.close();
            return integerOut;
        }
    }

    //创建表格不支持类型写成bool
    public void createBoolTable() throws SQLException, ClassNotFoundException {
        String strBooleanTableName = getBooleanTableName();
        try(Statement statement = connection.createStatement()) {
            String createBLTableSQL = "create table booltest" + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "address varchar(255),"
                    + "is_delete bool,"
                    + "primary key(id)"
                    + ")";
            statement.execute(createBLTableSQL);
        }
    }
}
