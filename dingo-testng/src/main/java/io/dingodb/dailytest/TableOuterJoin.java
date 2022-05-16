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

public class TableOuterJoin {
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

    //创建student_tbl表
    public void createStudentTable(String studentMeta) throws SQLException {
        Statement statement = connection.createStatement();
        String createStudentTableSQL = "create table student_tbl" + studentMeta;

        statement.execute(createStudentTableSQL);
        statement.close();
    }

    //向student_tbl表插入数据
    public void insertValuesToStudent(String studentValues) throws SQLException {
        Statement statement = connection.createStatement();
        String insertStudentValuesSQL = "insert into student_tbl values " + studentValues;

        statement.executeUpdate(insertStudentValuesSQL);
        statement.close();
    }

    //创建class_tbl表
    public void createClassTable(String classMeta) throws SQLException {
        Statement statement = connection.createStatement();
        String createClassTableSQL = "create table class_tbl" + classMeta;

        statement.execute(createClassTableSQL);
        statement.close();
    }

    //向class_tbl表插入数据
    public void insertValuesToClass(String classValues) throws SQLException {
        Statement statement = connection.createStatement();
        String insertClassValuesSQL = "insert into class_tbl values " + classValues;

        statement.executeUpdate(insertClassValuesSQL);
        statement.close();
    }

    //创建student_tbl1空表
    public void createStudent1Table(String studentMeta) throws SQLException {
        Statement statement = connection.createStatement();
        String createStudentTableSQL = "create table student_tbl1" + studentMeta;

        statement.execute(createStudentTableSQL);
        statement.close();
    }

    //创建class_tbl1空表
    public void createClass1Table(String classMeta) throws SQLException {
        Statement statement = connection.createStatement();
        String createClassTableSQL = "create table class_tbl1" + classMeta;

        statement.execute(createClassTableSQL);
        statement.close();
    }

    //创建product1表
    public void createProduct1Table(String product1Meta) throws SQLException {
        Statement statement = connection.createStatement();
        String createProduct1TableSQL = "create table product1" + product1Meta;

        statement.execute(createProduct1TableSQL);
        statement.close();
    }

    //向product1表插入数据
    public void insertValuesToProduct1(String product1Values) throws SQLException {
        Statement statement = connection.createStatement();
        String insertProduct1ValuesSQL = "insert into product1 values " + product1Values;

        statement.executeUpdate(insertProduct1ValuesSQL);
        statement.close();
    }

    //创建product2表
    public void createProuct2Table(String product2Meta) throws SQLException {
        Statement statement = connection.createStatement();
        String createProduct2TableSQL = "create table product2" + product2Meta;

        statement.execute(createProduct2TableSQL);
        statement.close();
    }

    //向product2表插入数据
    public void insertValuesToProduct2(String product2Values) throws SQLException {
        Statement statement = connection.createStatement();
        String insertProduct2ValuesSQL = "insert into product2 values " + product2Values;

        statement.executeUpdate(insertProduct2ValuesSQL);
        statement.close();
    }

    //创建test1表
    public void createTest1Table(String test1Meta) throws SQLException {
        Statement statement = connection.createStatement();
        String createTest1TableSQL = "create table test1" + test1Meta;

        statement.execute(createTest1TableSQL);
        statement.close();
    }

    //向test1表插入数据
    public void insertValuesToTest1(String test1Values) throws SQLException {
        Statement statement = connection.createStatement();
        String insertTest1ValuesSQL = "insert into test1 values " + test1Values;

        statement.executeUpdate(insertTest1ValuesSQL);
        statement.close();
    }

    //创建test2表
    public void createTest2Table(String test2Meta) throws SQLException {
        Statement statement = connection.createStatement();
        String createTest2TableSQL = "create table test2" + test2Meta;

        statement.execute(createTest2TableSQL);
        statement.close();
    }

    //向test2表插入数据
    public void insertValuesToTest2(String test2Values) throws SQLException {
        Statement statement = connection.createStatement();
        String insertTest2ValuesSQL = "insert into test2 values " + test2Values;

        statement.executeUpdate(insertTest2ValuesSQL);
        statement.close();
    }

    public List<List> fullOuterJoinAll() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select s.*,c.* from student_tbl s full outer join class_tbl c on s.class_id=c.cid";
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

    public List<List> fullOuterJoinOmitOuter() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full join " +
                "class_tbl on student_tbl.class_id=class_tbl.cid";
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

    public void fullOuterJoinWithoutCondition() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full join class_tbl";
        statement.executeQuery(querySQL);

        statement.close();
    }

    public List<List> fullOuterJoinNoSameData() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select product1.*,product2.* from product1 full outer join product2 on product1.id=product2.id";
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

    public List<List> fullOuterJoinOneEmpty() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select s.*,c1.* from student_tbl s full join class_tbl1 c1 on s.class_id=c1.cid";
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

    public List<List> fullOuterJoinUsingKeyState() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select test1.*,test2.* from test1 full outer join test2 using(id)";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            rowList.add(resultSet.getString(4));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    public void fullOuterJoinWrongKey() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select product1.*,product2.* from product1 full outer join " +
                "product2 on product1.buyers = product2.buyers";
        statement.executeQuery(querySQL);
        statement.close();
    }

    public List<List> fullOuterJoinStarQueryAll() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select * from test1 full outer join test2 on test1.id=test2.id";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            rowList.add(resultSet.getString(4));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    public void fullOuterJoinSameIDWithoutTablePrefix() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select id from test1 full outer join test2 on test1.id=test2.id";
        statement.executeQuery(querySQL);
        statement.close();
    }

    public List<List> fullOuterJoinUniqueFieldQuery() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select sid,name,cname from student_tbl full outer join class_tbl on student_tbl.class_id=class_tbl.cid";
        ResultSet resultSet = statement.executeQuery(querySQL);
        List<List> queryList = new ArrayList<List>();

        while(resultSet.next()) {
            List rowList = new ArrayList ();
            rowList.add(resultSet.getString(1));
            rowList.add(resultSet.getString(2));
            rowList.add(resultSet.getString(3));
            queryList.add(rowList);
        }
        statement.close();
        return queryList;
    }

    public List<List> fullOuterJoinExchangeTable() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select student_tbl.*,class_tbl.* from class_tbl full outer join " +
                "student_tbl on student_tbl.class_id=class_tbl.cid";
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

    public List<List> fullOuterJoinMatchRow() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select s.*,c.* from student_tbl s full outer join class_tbl c on s.class_id=c.cid " +
                "where s.sid is not null and c.cid is not null";
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

    public List<List> fullOuterJoinNotMatchRow() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select s.*,c.* from student_tbl s full outer join class_tbl c on s.class_id=c.cid " +
                "where s.sid is null or c.cid is null";
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

    public void fullOuterJoinUsingKeyNoSameField() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full outer join class_tbl using(id)";
        statement.executeQuery(querySQL);
        statement.close();
    }

    public Boolean fullOuterJoinBothEmpty() throws SQLException {
        Statement statement = connection.createStatement();
        String querySQL = "select student_tbl1.*,class_tbl1.* from student_tbl1 " +
                "full outer join class_tbl1 on student_tbl1.class_id=class_tbl1.cid";
        ResultSet resultSet = statement.executeQuery(querySQL);

//        statement.close();
        return resultSet.next();
    }

}
