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

public class TableOuterJoin {
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

    public List<List> leftOuterJoinOnlyInLeft() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select beauty_tbl.name,boyfriend_id from beauty_tbl left outer join " +
                    "boys_tbl on boys_tbl.id=beauty_tbl.boyfriend_id where boys_tbl.id is null";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    public List<List> leftOuterJoinAllData() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select beauty_tbl.*,boys_tbl.* from beauty_tbl left outer join " +
                    "boys_tbl on boys_tbl.id=beauty_tbl.boyfriend_id";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getString(7));
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接省略outer
    public List<List> leftOuterJoinOmitOuter() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT d.*,e.employee_id\n" +
                    "FROM departments_tbl d\n" +
                    "LEFT JOIN employees_tbl e\n" +
                    "ON d.department_id = e.department_id\n" +
                    "WHERE e.employee_id IS NULL";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接两表无交集
    public List<List> leftOuterJoinNoSameData() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.*,product2.* from product1 left join product2 on product1.id = product2.id";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接字段不存在
    public void leftOuterJoinWrongKey() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.*,product2.* from product1 left join product2 on product1.buyers = product2.buyers";
            statement.executeQuery(querySQL);
        }
    }

    //左表无数据
     public Boolean leftOuterJoinNoDataLeft() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product3.* from product3 left join product1 on product1.id=product3.id";
            ResultSet resultSet = statement.executeQuery(querySQL);
            Boolean queryResult = resultSet.next();
            resultSet.close();
            statement.close();
            return queryResult;
        }
    }

    //左连接右表无数据
    public List<List> leftOuterJoinNoDataRight() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.* from product1 left join product3 on product3.id=product1.id";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
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

    //左连接where条件1
    public List<List> leftOuterJoinWhereState1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT beauty_tbl.id,beauty_tbl.name from beauty_tbl left join boys_tbl on " +
                    "boys_tbl.id = beauty_tbl.boyfriend_id where boys_tbl.id is null and beauty_tbl.id<7";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接where条件2
    public List<List> leftOuterJoinWhereState2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from class_tbl left join student_tbl " +
                    "on student_tbl.class_id=class_tbl.cid where student_tbl.sid=1";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接using(key)用法
    public List<List> leftOuterJoinUsingKey() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select test1.*,test2.* from test1 left join test2 using(id)";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //左连接缺少连接条件
    public void leftOuterJoinMissingCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select beauty_tbl.name,boyfriend_id from beauty_tbl left outer join boys_tbl";
            statement.executeQuery(querySQL);
        }
    }

    //左连接复合查询
    public List<List> leftOuterJoinMixQuery() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT beauty_tbl.borndate,count(borndate) cb from beauty_tbl" +
                    " left join boys_right on boys_right.id=beauty_tbl.boyfriend_id " +
                    "where boys_right.id is not null and beauty_tbl.name<>'Angelay' " +
                    "group by borndate order by cb";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //右连接仅查询右表数据
    public List<List> rightOuterJoinOnlyInRight() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select beauty_tbl.boyfriend_id,boys_right.id,boys_right.boyName from beauty_tbl right join boys_right on " +
                    "boys_right.id=beauty_tbl.boyfriend_id where beauty_tbl.boyfriend_id is null";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
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

    //右连接返回全部数据
    public List<List> rightOuterJoinAllData() throws SQLException, InterruptedException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select w3cschool_tbl.*,tcount_tbl.* from w3cschool_tbl right outer join tcount_tbl on" +
                    " tcount_tbl.w3cschool_author=w3cschool_tbl.w3cschool_author";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));

                queryList.add(rowList);
            }
            resultSet.close();
            Thread.sleep(2000);
            statement.close();
            return queryList;
        }
    }

    //右连接省略outer
    public List<List> rightOuterJoinOmitOuter() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT d.*,e.employee_id\n" +
                    "FROM employees_tbl e\n" +
                    "RIGHT JOIN departments_tbl d\n" +
                    "ON d.department_id = e.department_id\n" +
                    "WHERE e.employee_id IS NULL";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //右连接两表无交集
    public List<List> rightOuterJoinNoSameData() throws SQLException, InterruptedException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.*,product2.* from product1 right join product2 on product1.id = product2.id";
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
            resultSet.close();
            Thread.sleep(2000);
            statement.close();
            return queryList;
        }
    }

    //右连接字段不存在
    public void rightOuterJoinWrongKey() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.*,product2.* from product1 right join product2 on product1.buyers = product2.buyers";
            statement.executeQuery(querySQL);
        }
    }

    //右连接左表无数据
    public List<List> rightOuterJoinNoDataLeft() throws SQLException, InterruptedException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.* from product3 right join product1 on product1.id=product3.id";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                queryList.add(rowList);
            }
            Thread.sleep(2000);
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //右表无数据
    public Boolean rightOuterJoinNoDataRight() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product3.* from product1 right join product3 on product3.id=product1.id";
            ResultSet resultSet = statement.executeQuery(querySQL);
            Boolean queryResult = resultSet.next();
            resultSet.close();
            statement.close();
            return queryResult;
        }
    }

    //右连接where条件1
    public String rightOuterJoinWhereState1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT boys_right.boyName from beauty_tbl right join boys_right on boys_right.id=beauty_tbl.boyfriend_id " +
                    "where beauty_tbl.boyfriend_id is null and boys_right.id>5";
            ResultSet resultSet = statement.executeQuery(querySQL);
            String whereResult = null;

            while(resultSet.next()) {
                whereResult = resultSet.getString(1);
            }
            resultSet.close();
            statement.close();
            return whereResult;
        }
    }

    //右连接where条件2
    public List<List> rightOuterJoinWhereState2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl right join class_tbl on " +
                    "student_tbl.class_id=class_tbl.cid where student_tbl.sid=1";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //右连接缺少连接条件
    public void rightOuterJoinMissingCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select boys_tbl.* from beauty_tbl right outer join boys_tbl where beauty_tbl.boyfriend_id is null";
            statement.executeQuery(querySQL);
        }
    }

    //右连接using(key)用法
    public List<List> rightOuterJoinUsingKey() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select test1.*,test2.* from test1 right join test2 using(id)";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //右连接复合查询
    public List<List> rightOuterJoinMixQuery() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "SELECT boys_right.boyName,count(boys_right.boyName) cn from beauty_tbl" +
                    " right join boys_right on boys_right.id=beauty_tbl.boyfriend_id" +
                    " where beauty_tbl.boyfriend_id is not null and beauty_tbl.id<10" +
                    " and boys_right.boyName not in('Han Han','DuanYU') group by boyName order by cn desc";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
                rowList.add(resultSet.getString("boyName"));
                rowList.add(resultSet.getString("cn"));
                queryList.add(rowList);
            }
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    public List<List> fullOuterJoinAll() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    public List<List> fullOuterJoinOmitOuter() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    public void fullOuterJoinWithoutCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full join class_tbl";
            statement.executeQuery(querySQL);
        }
    }

    //全连接where条件1
    public List<List> fullOuterJoinWhereState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full join class_tbl on " +
                    "student_tbl.class_id=class_tbl.cid where student_tbl.sid=1";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证两表无相同数据时，全连接
    public List<List> fullOuterJoinNoSameData() throws SQLException, InterruptedException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            Thread.sleep(3000);
            statement.close();
            return queryList;
        }
    }

    //验证一表为空时全连接
    public List<List> fullOuterJoinOneEmpty() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //全连接使用using(key)表达式
    public List<List> fullOuterJoinUsingKeyState() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //其中一个表中不存在连接使用的字段名，返回错误
    public void fullOuterJoinWrongKey() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select product1.*,product2.* from product1 full outer join " +
                    "product2 on product1.buyers = product2.buyers";
            statement.executeQuery(querySQL);
        }
    }

    //使用*查询所有
    public List<List> fullOuterJoinStarQueryAll() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证查询相同字段，不使用表名修饰预期失败
    public void fullOuterJoinSameIDWithoutTablePrefix() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select id from test1 full outer join test2 on test1.id=test2.id";
            statement.executeQuery(querySQL);
        }
    }

    //查询独有字段可不使用表名修饰
    public List<List> fullOuterJoinUniqueFieldQuery() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证表名位置互换查询结果相同
    public List<List> fullOuterJoinExchangeTable() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //查询两表匹配到的数据
    public List<List> fullOuterJoinMatchRow() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //查询两表未匹配到的数据
    public List<List> fullOuterJoinNotMatchRow() throws SQLException {
        try(Statement statement = connection.createStatement()) {
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证使用using(key)语句进行全外连接查询，两表并没有相同的字段key，预期异常
    public void fullOuterJoinUsingKeyNoSameField() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl full outer join class_tbl using(id)";
            statement.executeQuery(querySQL);
        }
    }

    //验证两个表都为空，进行全连接查询
    public Boolean fullOuterJoinBothEmpty() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl1.*,class_tbl1.* from student_tbl1 " +
                    "full outer join class_tbl1 on student_tbl1.class_id=class_tbl1.cid";
            ResultSet resultSet = statement.executeQuery(querySQL);
            Boolean queryResult = resultSet.next();
            resultSet.close();
            statement.close();
            return queryResult;
        }
    }

    //验证交叉连接查询两表的全部数据，使用表1 cross join表2的语法
    public List<List> crossJoinAll() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl cross join class_tbl";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证交叉连接查询两表的全部数据，使用表1,表2的语法
    public List<List> crossJoinAllSeprateComma() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl,class_tbl";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证交叉连接添加等值条件，返回错误
    public void crossJoinExtraCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl " +
                    "cross join class_tbl on student_tbl.class_id=class_tbl.cid";
            statement.executeQuery(querySQL);
        }
    }

    //验证交叉连接使用表1 cross join 表2的语法查询并带有where条件
    public List<List> crossJoinWhereCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl " +
                    "cross join class_tbl where student_tbl.class_id=class_tbl.cid";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证交叉连接使用表1,表2的语法查询并带有where条件
    public List<List> crossJoinCommaWhereCondition() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl.* from student_tbl,class_tbl where student_tbl.class_id=class_tbl.cid";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //验证其中一个表为空，交叉连接查询返回空
    public Boolean crossJoinOneEmpty() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select student_tbl.*,class_tbl1.* from student_tbl cross join class_tbl1";
            ResultSet resultSet = statement.executeQuery(querySQL);
            Boolean queryResult = resultSet.next();
            resultSet.close();
            statement.close();
            return queryResult;
        }
    }

    //使用*查询全部数据，使用表1 cross join 表2的语法
    public List<List> crossJoinStarQueryAll1() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from test1 cross join test2";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //使用*查询全部数据，使用表1,表2的语法
    public List<List> crossJoinStarQueryAll2() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select * from test1,test2";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }

    //连接的两个表都含有相同的字段名，查询该字段不使用表名修饰，返回错误
    public Boolean crossJoinSameFieldNoTablePrefix() throws SQLException {
        try(Statement statement = connection.createStatement();) {
            String querySQL = "select id from test1 cross join test2";
            ResultSet resultSet = statement.executeQuery(querySQL);
            Boolean queryResult = resultSet.next();
            resultSet.close();
            statement.close();
            return queryResult;
        }
    }

    //验证查询字段若是表中独有字段，可省略表名修饰
    public List<List> crossJoinUniqueFieldNoTablePrefix() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select sid,name,cname from student_tbl cross join class_tbl";
            ResultSet resultSet = statement.executeQuery(querySQL);
            List<List> queryList = new ArrayList<List>();

            while(resultSet.next()) {
                List rowList = new ArrayList ();
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

    //调换位置做交叉连接，笛卡尔积结果不同
    public List<List> crossJoinExchangeTable() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String querySQL = "select s.*,c.* from class_tbl c cross join student_tbl s";
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
            resultSet.close();
            statement.close();
            return queryList;
        }
    }
}
