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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TableInnerJoin {
    public static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建测试表
    public void createTableWithMeta(String tableName, String tableMeta) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createTableSQL = "create table " + tableName +  tableMeta;
            statement.execute(createTableSQL);
        }
    }

    //表数据插入
    public void insertTableValues(String tableName, String insertFields, String tableValue) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertValuesSQL = "insert into " + tableName + insertFields + " values " + tableValue;
            statement.execute(insertValuesSQL);
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
            joinRst.close();
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
            joinAliasRst.close();
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
            joinWithTablePrefixRst.close();
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
            tableExchangeRst.close();
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
            innerJoinGroupRst.close();
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
            innerJoinWhereRst.close();
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
            joinGroupAndOrderRst.close();
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
            joinGroupAndOrderLimitRst.close();
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
            joinOmitInnerRst.close();
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
            joinNoSameDataRst.close();
            statement.close();
            return joinNoSameDataList;
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

            selfJoinRst.close();
            statement.close();
            return selfJoinList;
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

            joinRst.close();
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

            joinRst.close();
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
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1069_1.* from table1069_1 inner join table1069_2 on table1069_2.id=table1069_1.id";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult = joinRst.next();
            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
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
        try(Statement statement = connection.createStatement()) {
            String querySql = "select table1059_1.*,table1059_2.* from table1059_1 inner join table1059_2 " +
                    "on table1059_1.buyers>table1059_2.price";
            ResultSet joinRst = statement.executeQuery(querySql);
            Boolean queryResult =  joinRst.next();
            joinRst.close();
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
            resultSet.close();
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

            joinRst.close();
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
            joinRst.close();
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
            joinRst.close();
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

            joinRst.close();
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

            joinRst.close();
            statement.close();
            return joinList;
        }
    }
}
