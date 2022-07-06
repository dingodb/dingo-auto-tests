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

    public void createInnerTables() throws SQLException {
        String innerTable1 = "beauty";
        String innerTable2 = "boys";
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    public void insertDataToInnerTables() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    public void createEmployeesTables() throws SQLException {
        String departmentTable = "departments";
        String employeeTable = "employees";
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    public void insertValuesToEmployeeTables(String departmentValues, String employeeValues) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertdeptTableSQL = "insert  into departments(department_id,department_name,manager_id,location_id)" +
                    " values " + departmentValues;
            String insertempTableSQL = "insert into employees(employee_id,first_name,last_name,email,phone_number," +
                    "job_id,salary,commission_pct,manager_id,department_id,hiredate) values " + employeeValues;

            statement.execute(insertdeptTableSQL);
            statement.execute(insertempTableSQL);
        }
    }

    //创建job_grades表
    public void createJobGradesTable(String job_grades_Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table job_grades" + job_grades_Meta;
            statement.execute(createTableSQL);
        }
    }

    //向job_grades表插入数据
    public void insertValuesToJobGrades(String job_grades_Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into job_grades values " + job_grades_Values;
            statement.executeUpdate(insertValuesSQL);
        }
    }

    public List<List<String>> innerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String joinSQL = "select name, boyname from beauty inner join boys on beauty.boyfriend_id = boys.id";
            ResultSet joinRst = statement.executeQuery(joinSQL);
            List<List<String>> joinList = new ArrayList<List<String>>();
            while (joinRst.next()) {
                List<String> equalRowList = new ArrayList<String>();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                joinList.add(equalRowList);
            }
            statement.close();
            return joinList;
        }
    }

    public List<List<String>> innerJoinWithTableAlias () throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String joinAliasSQL = "select g.name, b.boyname from beauty as g inner join boys as b on g.boyfriend_id = b.id";
            ResultSet joinAliasRst = statement.executeQuery(joinAliasSQL);
            List<List<String>> joinAliasList = new ArrayList<List<String>>();
            while (joinAliasRst.next()) {
                List<String> equalRowAliasList = new ArrayList<String>();
                equalRowAliasList.add(joinAliasRst.getString(1));
                equalRowAliasList.add(joinAliasRst.getString(2));
                joinAliasList.add(equalRowAliasList);
            }
            statement.close();
            return joinAliasList;
        }
    }

    public List<List<String>> innerJoinSameFieldWithTablePrefix () throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            statement.close();
            return joinWithTablePrefixList;
        }
    }

    public List<List<String>> innerJoinTableExchange() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String tableExchangeSQL = "select name, boyname from boys inner join beauty on boys.id=beauty.boyfriend_id";
            ResultSet tableExchangeRst = statement.executeQuery(tableExchangeSQL);
            List<List<String>> tableExchangeList = new ArrayList<List<String>>();
            while (tableExchangeRst.next()) {
                List<String> equalRowList = new ArrayList<String>();
                equalRowList.add(tableExchangeRst.getString(1));
                equalRowList.add(tableExchangeRst.getString(2));
                tableExchangeList.add(equalRowList);
            }
            statement.close();
            return tableExchangeList;
        }
    }

    public List<List<String>> innerJoinGroupState() throws SQLException {
        try(Statement groupStatement = connection.createStatement()) {
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
            groupStatement.close();
            return innerJoinGroupList;
        }
    }

    public List<List<String>> innerJoinWhereState() throws SQLException {
        try(Statement whereStatement = connection.createStatement()) {
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
            whereStatement.close();
            return innerJoinWhereList;
        }
    }

    public List<List<String>> innerJoinGroupAndOrder() throws SQLException {
        try(Statement joinGroupAndOrderStatement = connection.createStatement()) {
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
            joinGroupAndOrderStatement.close();
            return joinGroupAndOrderList;
        }
    }

    public List<List<String>> innerJoinGroupAndOrderLimit() throws SQLException {
        try(Statement joinGroupAndOrderLimitStatement = connection.createStatement()) {
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
            joinGroupAndOrderLimitStatement.close();
            return joinGroupAndOrderLimitList;
        }
    }

    public List<List<String>> joinOmitInner() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String joinOmitInnerSQL = "select name, boyname from beauty join boys on boys.id=beauty.boyfriend_id";
            ResultSet joinOmitInnerRst = statement.executeQuery(joinOmitInnerSQL);
            List<List<String>> joinOmitInnerList = new ArrayList<List<String>>();
            while (joinOmitInnerRst.next()) {
                List<String> equalRowList = new ArrayList<String>();
                equalRowList.add(joinOmitInnerRst.getString(1));
                equalRowList.add(joinOmitInnerRst.getString(2));
                joinOmitInnerList.add(equalRowList);
            }
            statement.close();
            return joinOmitInnerList;
        }
    }

    public List<String> innerJoinNoSameData() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String innerJoinNoSameDataSQL = "select beauty.name from beauty inner join boys on beauty.name = boys.boyName";
            ResultSet joinNoSameDataRst = statement.executeQuery(innerJoinNoSameDataSQL);
            List<String> joinNoSameDataList = new ArrayList<String>();
            while (joinNoSameDataRst.next()) {
                joinNoSameDataList.add(joinNoSameDataRst.getString(1));
            }
            statement.close();
            return joinNoSameDataList;
        }
    }

    public void createSelfJoinTable() throws SQLException {
        try(Statement createSelfJoinstatement = connection.createStatement()) {
            String createSelfJoinsql = "create table mytest (id int, name varchar(20), manager_id int, primary key(id))";
            createSelfJoinstatement.execute(createSelfJoinsql);
        }
    }

    public void insertValuesToSelftJoinTable(String selfValues) throws SQLException {
        try(Statement insertSelfJoinstatement = connection.createStatement()) {
            String insertSelfJoinsql = "insert into mytest values " + selfValues;
            insertSelfJoinstatement.execute(insertSelfJoinsql);
        }
    }

    public List<List<String>> selfJoin() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String selfJoinsql = "select a.name,b.name from mytest a inner join mytest b on a.manager_id=b.id";
            ResultSet selfJoinRst = statement.executeQuery(selfJoinsql);
            List<List<String>> selfJoinList = new ArrayList<List<String>>();
            while (selfJoinRst.next()) {
                List<String> equalRowList = new ArrayList<String>();
                equalRowList.add(selfJoinRst.getString(1));
                equalRowList.add(selfJoinRst.getString(2));
                selfJoinList.add(equalRowList);
            }

            statement.close();
            return selfJoinList;
        }
    }

    //创建table1054_1表
    public void createTable1054_1(String table10541Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table table1054_1" + table10541Meta;
            statement.execute(createTableSQL);
        }
    }

    //向table1054_1表插入数据
    public void insertValuesToTable1054_1(String table10541Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into table1054_1 values " + table10541Values;
            statement.executeUpdate(insertValuesSQL);
        }
    }

    //创建table1054_2表
    public void createTable1054_2(String table10542Meta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table table1054_2" + table10542Meta;
            statement.execute(createTableSQL);
        }
    }

    //向table1054_2表插入数据
    public void insertValuesToTable1054_2(String table10542Values) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into table1054_2 values " + table10542Values;
            statement.executeUpdate(insertValuesSQL);
        }
    }

    //验证自然连接
    public List<List> naturalJoin() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1054_1.snum, sname, ssex, sdept, cno, grade from table1054_1" +
                    " inner join table1054_2 on table1054_1.snum=table1054_2.snum";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                equalRowList.add(joinRst.getString(6));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内等连接，使用逗号分隔表名，并添加where条件进行连接
    public List<List> innerJoinCommaWhere() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select name, boyname from beauty,boys where beauty.boyfriend_id = boys.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    public void createTable1069() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql1 = "create table table1069_1 (id int, buyers int, price int, primary key(id))";
            String createSql2 = "create table table1069_2 (id int, price int, primary key(id))";
            String insertSql1 = "insert into table1069_1 values(1,2,3),(4,5,6),(7,8,9)";
            statement.execute(createSql1);
            statement.execute(createSql2);
            statement.execute(insertSql1);
        }
    }

    //验证某一表无数据返回空
    public Boolean innerJoinOneEmpty() throws SQLException {
        createTable1069();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1069_1.* from table1069_1 inner join table1069_2 on table1069_2.id=table1069_1.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult = joinRst.next();
            statement.close();
            return queryResult;
        }
    }

    //创建表，有相同字段
    public void createTable1174() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql1 = "create table table1174_1 (id int, name varchar(20), age int, primary key(id))";
            String createSql2 = "create table table1174_2 (id int, name varchar(20), age int, primary key(id))";
            String insertSql1 = "insert into table1174_1 values(1,'zhangsan',18),(2,'lisi',20),(3,'wangwu',25),(4,'liuming',18)";
            String insertSql2 = "insert into table1174_2 values(1,'zhangsan',18),(3,'hello',35),(5,'wangwu',25),(6,'Haha',20)";
            statement.execute(createSql1);
            statement.execute(createSql2);
            statement.execute(insertSql1);
            statement.execute(insertSql2);
        }
    }

    //验证内等连接，使用using(key)
    public List<List> innerJoinUsingKey() throws SQLException {
        createTable1174();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1174_1.*,table1174_2.* from table1174_1 inner join table1174_2 using(id)";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                equalRowList.add(joinRst.getString(6));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接多个条件
    public List<List> innerNEJoinMultiCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "SELECT salary,grade_level from employees e " +
                    "inner JOIN job_grades g ON e.salary>=g.lowest_sal AND e.salary <=g.highest_sal " +
                    "where e.employee_id<118";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接添加分组
    public List<List> innerNEJoinWithGroup() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "SELECT COUNT(*),grade_level\n" +
                    "FROM employees e\n" +
                    "JOIN job_grades g\n" +
                    "ON e.salary>=g.lowest_sal AND e.salary<=g.highest_sal\n" +
                    "GROUP BY grade_level";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接添加排序
    public List<List> innerNEJoinWithOrder() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "SELECT COUNT(*) cn,grade_level FROM employees e inner JOIN job_grades g " +
                    "ON e.salary>=g.lowest_sal AND e.salary<=g.highest_sal " +
                    "GROUP BY grade_level ORDER BY grade_level DESC";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接小于条件
    public List<List> innerNEJoinST() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1174_1.*,table1174_2.* from table1174_1 inner join table1174_2" +
                    " on table1174_1.age<table1174_2.age";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                equalRowList.add(joinRst.getString(6));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接大于条件
    public List<List> innerNEJoinLT() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1174_1.*,table1174_2.* from table1174_1 inner join table1174_2" +
                    " on table1174_1.age>table1174_2.age";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                equalRowList.add(joinRst.getString(6));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接不等于条件
    public List<List> innerNEJoinNE() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1174_1.*,table1174_2.* from table1174_1 inner join table1174_2" +
                    " on table1174_1.age<>table1174_2.age";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                equalRowList.add(joinRst.getString(6));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //创建表，非等值连接
    public void createTable1059() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSql1 = "create table table1059_1 (id int, buyers int, price int, primary key(id))";
            String createSql2 = "create table table1059_2 (id int, price int, primary key(id))";
            String createSql3 = "create table table1059_3 (id int, price int, primary key(id))";
            String createSql4 = "create table table1059_4 (id int, price int, primary key(id))";
            String insertSql1 = "insert into table1059_1 values(1,2,3),(4,5,6),(7,8,9)";
            String insertSql2 = "insert into table1059_2 values(8,9),(10,11)";
            String insertSql4 = "insert into table1059_4 values(3,1),(6,2)";
            statement.execute(createSql1);
            statement.execute(createSql2);
            statement.execute(createSql3);
            statement.execute(createSql4);
            statement.execute(insertSql1);
            statement.execute(insertSql2);
            statement.execute(insertSql4);
        }
    }

    //验证内非连接无符合条件返回空
    public Boolean innerNEJoinNoDataQuery() throws SQLException {
        createTable1059();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.*,table1059_2.* from table1059_1 inner join table1059_2 " +
                    "on table1059_1.buyers>table1059_2.price";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult =  joinRst.next();
            statement.close();
            return queryResult;
        }
    }

    //验证内非连接找不到字段
    public void innerNEJoinNFieldNotExist() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.*,table1059_2.* from table1059_1 inner join table1059_2 " +
                    "on table1059_1.buyers<table1059_2.buyers";
            ResultSet resultSet = statement.executeQuery(querySql);
        }
    }

    //验证内非连接添加where条件
    public List<List> innerNEJoinWithWhere() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select bu.id gid,name,bu.boyfriend_id,boys.id bid,boyName from beauty as bu " +
                    "inner join boys on boys.id<>bu.boyfriend_id where bu.id>8";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString("gid"));
                equalRowList.add(joinRst.getString("name"));
                equalRowList.add(joinRst.getString("boyfriend_id"));
                equalRowList.add(joinRst.getString("bid"));
                equalRowList.add(joinRst.getString("boyName"));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接某一个表为空返回空
    public Boolean innerNEJoinOneEmpty_1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.* from table1059_1 inner join table1059_3 " +
                    "on table1059_3.id<table1059_1.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult = joinRst.next();
            statement.close();
            return queryResult;
        }
    }

    //验证内非连接某一个表为空返回空
    public Boolean innerNEJoinOneEmpty_2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_3.* from table1059_3 inner join table1059_1 " +
                    "on table1059_1.id<>table1059_3.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult = joinRst.next();
            statement.close();
            return queryResult;
        }
    }

    //验证内非连接两表互换
    public List<List> innerNEJoinExchangeTable1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.*,table1059_4.* from table1059_1" +
                    " inner join table1059_4 on table1059_1.buyers<table1059_4.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }

    //验证内非连接两表互换
    public List<List> innerNEJoinExchangeTable2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.*,table1059_4.* from table1059_4" +
                    " inner join table1059_1 on table1059_1.buyers<table1059_4.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            List<List> joinList = new ArrayList<List>();
            while (joinRst.next()) {
                List equalRowList = new ArrayList();
                equalRowList.add(joinRst.getString(1));
                equalRowList.add(joinRst.getString(2));
                equalRowList.add(joinRst.getString(3));
                equalRowList.add(joinRst.getString(4));
                equalRowList.add(joinRst.getString(5));
                joinList.add(equalRowList);
            }

            statement.close();
            return joinList;
        }
    }
}
