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

public class TableInnerJoin {
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

    public void createInnerTables() throws SQLException {
        String innerTable1 = "beauty";
        String innerTable2 = "boys";
        Statement statement = connection.createStatement();
        String createSQL1 = "create table " + innerTable1 + "(id int NOT NULL,\n" +
                "name varchar(50) NOT NULL,\n" +
                "sex varchar(10) DEFAULT 'female',\n" +
                "borndate timestamp DEFAULT '1987-01-01 00:00:00',\n" +
                "phone varchar(11) NOT NULL,\n" +
                "boyfriend_id int DEFAULT NULL,\n" +
                "PRIMARY KEY (id)\n" +
                ")";

        String createSQL2 = "create table " + innerTable2 + "(\n" +
                "id int NOT NULL,\n" +
                "boyName varchar(20) DEFAULT NULL,\n" +
                "userCP int DEFAULT NULL,\n" +
                "PRIMARY KEY (id)\n" +
                ")";
        statement.execute(createSQL1);
        statement.execute(createSQL2);
        statement.close();
    }

    public void insertDataToInnerTables() throws SQLException {
        Statement statement = connection.createStatement();
        String table1Values = "(1,'LiuYan','female','1988-02-03 00:00:00','18209876577',8),\n" +
                "(2,'TeacherLi','female','1987-12-30 00:00:00','18219876577',9),\n" +
                "(3,'Angelay','female','1989-02-03 00:00:00','18209876567',3),\n" +
                "(4,'ReBa','female','1993-02-03 00:00:00','18209876579',2),\n" +
                "(5,'DuLala','female','1992-02-03 00:00:00','18209179577',9),\n" +
                "(6,'zhiRuo','female','1988-02-03 00:00:00','18209876577',1),\n" +
                "(7,'LingShan','female','1987-12-30 00:00:00','18219876577',9),\n" +
                "(8,'Xiao Zhao','female','1989-02-03 00:00:00','18209876567',1),\n" +
                "(9,'Shuange','female','1993-02-03 00:00:00','18209876579',9),\n" +
                "(10,'Wang Yuyan','female','1992-02-03 00:00:00','18209179577',4),\n" +
                "(11,'Xia Xue','female','1993-02-03 00:00:00','18209876579',9),\n" +
                "(12,'Zhao Min','female','1992-02-03 00:00:00','18209179577',1)";

        String table2Values = "(1,'Zhang Wuji',100),(2,'Han Han',800),(3,'Xiao Ming',50),(4,'DuanYU',300)";

        String insertSQL1 = "insert into beauty values " + table1Values;
        String insertSQL2 = "insert into boys values " + table2Values;

        statement.execute(insertSQL1);
        statement.execute(insertSQL2);
        statement.close();
    }

    public void createEmployeesTables() throws SQLException {
        String departmentTable = "departments";
        String employeeTable = "employees";
        Statement statement = connection.createStatement();
        String createDepartmentSQL = "CREATE TABLE " + departmentTable +
                "  (department_id int NOT NULL,\n" +
                "  department_name varchar(10) DEFAULT NULL,\n" +
                "  manager_id int DEFAULT NULL,\n" +
                "  location_id int DEFAULT NULL,\n" +
                "  PRIMARY KEY (department_id))";

        String createEmployeeSQL = "CREATE TABLE " + employeeTable +
                "  (employee_id int NOT NULL,\n" +
                "  first_name varchar(20) DEFAULT NULL,\n" +
                "  last_name varchar(25) DEFAULT NULL,\n" +
                "  email varchar(25) DEFAULT NULL,\n" +
                "  phone_number varchar(20) DEFAULT NULL,\n" +
                "  job_id varchar(10) DEFAULT NULL,\n" +
                "  salary double DEFAULT NULL,\n" +
                "  commission_pct double DEFAULT NULL,\n" +
                "  manager_id int DEFAULT NULL,\n" +
                "  department_id int DEFAULT NULL,\n" +
                "  hiredate timestamp DEFAULT NULL,\n" +
                "  PRIMARY KEY (employee_id))";
        statement.execute(createDepartmentSQL);
        statement.execute(createEmployeeSQL);
        statement.close();
    }

    public void insertValuesToEmployeeTables(String departmentValues, String employeeValues) throws SQLException {
        Statement statement = connection.createStatement();
        String insertdeptTableSQL = "insert  into departments(department_id,department_name,manager_id,location_id)" +
                " values " + departmentValues;
        String insertempTableSQL = "insert into employees(employee_id,first_name,last_name,email,phone_number," +
                "job_id,salary,commission_pct,manager_id,department_id,hiredate) values " + employeeValues;

        statement.execute(insertdeptTableSQL);
        statement.execute(insertempTableSQL);
        statement.close();
    }

    public List<List<String>> innerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        Statement statement = connection.createStatement();
        String joinSQL = "select name, boyname from beauty inner join boys on beauty.boyfriend_id = boys.id";
        ResultSet joinRst = statement.executeQuery(joinSQL);
        List<List<String>> joinList = new ArrayList<List<String>>();
        while (joinRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(joinRst.getString(1));
            equalRowList.add(joinRst.getString(2));
            joinList.add(equalRowList);
        }
        return joinList;
    }

    public List<List<String>> innerJoinWithTableAlias () throws SQLException {
        Statement statement = connection.createStatement();
        String joinAliasSQL = "select g.name, b.boyname from beauty as g inner join boys as b on g.boyfriend_id = b.id";
        ResultSet joinAliasRst = statement.executeQuery(joinAliasSQL);
        List<List<String>> joinAliasList = new ArrayList<List<String>>();
        while (joinAliasRst.next()) {
            List<String> equalRowAliasList = new ArrayList<String>();
            equalRowAliasList.add(joinAliasRst.getString(1));
            equalRowAliasList.add(joinAliasRst.getString(2));
            joinAliasList.add(equalRowAliasList);
        }
        return joinAliasList;
    }

    public List<List<String>> innerJoinSameFieldWithTablePrefix () throws SQLException {
        Statement statement = connection.createStatement();
        String joinWithTablePrefixSQL = "select beauty.id,name,boys.id,boyname from beauty " +
                "inner join boys on beauty.boyfriend_id = boys.id";
        ResultSet joinWithTablePrefixRst = statement.executeQuery(joinWithTablePrefixSQL);
        List<List<String>> joinWithTablePrefixList = new ArrayList<List<String>>();
        while (joinWithTablePrefixRst.next()) {
            List<String> equalRowWithTablePrefixList = new ArrayList<String>();
            equalRowWithTablePrefixList.add(joinWithTablePrefixRst.getString(1));
            equalRowWithTablePrefixList.add(joinWithTablePrefixRst.getString(2));
            equalRowWithTablePrefixList.add(joinWithTablePrefixRst.getString(3));
            equalRowWithTablePrefixList.add(joinWithTablePrefixRst.getString(4));
            joinWithTablePrefixList.add(equalRowWithTablePrefixList);
        }
        return joinWithTablePrefixList;
    }

    public List<List<String>> innerJoinTableExchange() throws SQLException {
        Statement statement = connection.createStatement();
        String tableExchangeSQL = "select name, boyname from boys inner join beauty on boys.id=beauty.boyfriend_id";
        ResultSet tableExchangeRst = statement.executeQuery(tableExchangeSQL);
        List<List<String>> tableExchangeList = new ArrayList<List<String>>();
        while (tableExchangeRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(tableExchangeRst.getString(1));
            equalRowList.add(tableExchangeRst.getString(2));
            tableExchangeList.add(equalRowList);
        }
        return tableExchangeList;
    }

    public List<List<String>> innerJoinGroupState() throws SQLException {
        Statement groupStatement = connection.createStatement();
        String innerJoinGroupSQL = "select count(name) cn, boyname from beauty " +
                "inner join boys on boys.id=beauty.boyfriend_id group by boys.boyName";
        ResultSet innerJoinGroupRst = groupStatement.executeQuery(innerJoinGroupSQL);
        List<List<String>> innerJoinGroupList = new ArrayList<List<String>>();
        while (innerJoinGroupRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(innerJoinGroupRst.getString("cn"));
            equalRowList.add(innerJoinGroupRst.getString(2));
            innerJoinGroupList.add(equalRowList);
        }
        return innerJoinGroupList;
    }

    public List<List<String>> innerJoinWhereState() throws SQLException {
        Statement whereStatement = connection.createStatement();
        String innerJoinWhereSQL = "select name, boyname from beauty inner join boys on " +
                "beauty.boyfriend_id=boys.id where beauty.id<10";
        ResultSet innerJoinWhereRst = whereStatement.executeQuery(innerJoinWhereSQL);
        List<List<String>> innerJoinWhereList = new ArrayList<List<String>>();
        while (innerJoinWhereRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(innerJoinWhereRst.getString(1));
            equalRowList.add(innerJoinWhereRst.getString(2));
            innerJoinWhereList.add(equalRowList);
        }
        return innerJoinWhereList;
    }


    public List<List<String>> innerJoinGroupAndOrder() throws SQLException {
        Statement joinGroupAndOrderStatement = connection.createStatement();
        String joinGroupAndOrderSQL = "select count(*) cn,department_name from employees e inner join departments d" +
                " on e.department_id=d.department_id group by department_name order by cn desc";
        ResultSet joinGroupAndOrderRst = joinGroupAndOrderStatement.executeQuery(joinGroupAndOrderSQL);
        List<List<String>> joinGroupAndOrderList = new ArrayList<List<String>>();
        while (joinGroupAndOrderRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(joinGroupAndOrderRst.getString("cn"));
            equalRowList.add(joinGroupAndOrderRst.getString("department_name"));
            joinGroupAndOrderList.add(equalRowList);
        }
        return joinGroupAndOrderList;
    }

    public List<List<String>> innerJoinGroupAndOrderLimit() throws SQLException {
        Statement joinGroupAndOrderLimitStatement = connection.createStatement();
        String joinGroupAndOrderLimitSQL = "select count(*),department_name from employees e inner join departments d" +
                " on e.department_id=d.department_id group by department_name order by count(*) limit 5";
        ResultSet joinGroupAndOrderLimitRst = joinGroupAndOrderLimitStatement.executeQuery(joinGroupAndOrderLimitSQL);
        List<List<String>> joinGroupAndOrderLimitList = new ArrayList<List<String>>();
        while (joinGroupAndOrderLimitRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(joinGroupAndOrderLimitRst.getString(1));
            equalRowList.add(joinGroupAndOrderLimitRst.getString(2));
            joinGroupAndOrderLimitList.add(equalRowList);
        }
        return joinGroupAndOrderLimitList;
    }

    public List<List<String>> joinOmitInner() throws SQLException {
        Statement statement = connection.createStatement();
        String joinOmitInnerSQL = "select name, boyname from beauty join boys on boys.id=beauty.boyfriend_id";
        ResultSet joinOmitInnerRst = statement.executeQuery(joinOmitInnerSQL);
        List<List<String>> joinOmitInnerList = new ArrayList<List<String>>();
        while (joinOmitInnerRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(joinOmitInnerRst.getString(1));
            equalRowList.add(joinOmitInnerRst.getString(2));
            joinOmitInnerList.add(equalRowList);
        }
        return joinOmitInnerList;
    }

    public List<String> innerJoinNoSameData() throws SQLException {
        Statement statement = connection.createStatement();
        String innerJoinNoSameDataSQL = "select beauty.name from beauty inner join boys on beauty.name = boys.boyName";
        ResultSet joinNoSameDataRst = statement.executeQuery(innerJoinNoSameDataSQL);
        List<String> joinNoSameDataList = new ArrayList<String>();
        while (joinNoSameDataRst.next()) {
            joinNoSameDataList.add(joinNoSameDataRst.getString(1));
        }
        return joinNoSameDataList;
    }

    public void createSelfJoinTable() throws SQLException {
        Statement createSelfJoinstatement = connection.createStatement();
        String createSelfJoinsql = "create table mytest (id int, name varchar(20), manager_id int, primary key(id))";
        createSelfJoinstatement.execute(createSelfJoinsql);
        createSelfJoinstatement.close();

    }

    public void insertValuesToSelftJoinTable(String selfValues) throws SQLException {
        Statement insertSelfJoinstatement = connection.createStatement();
        String insertSelfJoinsql = "insert into mytest values " + selfValues;
        insertSelfJoinstatement.execute(insertSelfJoinsql);
        insertSelfJoinstatement.close();
    }

    public List<List<String>> selfJoin() throws SQLException {
        Statement statement = connection.createStatement();
        String selfJoinsql = "select a.name,b.name from mytest a inner join mytest b on a.id=b.manager_id";
        ResultSet selfJoinRst = statement.executeQuery(selfJoinsql);
        List<List<String>> selfJoinList = new ArrayList<List<String>>();
        while (selfJoinRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(selfJoinRst.getString(1));
            equalRowList.add(selfJoinRst.getString(2));
            selfJoinList.add(equalRowList);
        }
        return selfJoinList;
    }

}
