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

package io.dingodb.test;

import io.dingodb.dailytest.TableInnerJoin;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class TestTableInnerJoin {
    public static TableInnerJoin tableJoinObj = new TableInnerJoin();

    public static List<List<String>> girlsJoinList() {
        String[][] beautyBoyArray = {{"ReBa","Han Han"},{"zhiRuo","Zhang Wuji"}, {"Xiao Zhao", "Zhang Wuji"},
                {"Wang Yuyan", "DuanYU"}, {"Zhao Min", "Zhang Wuji"},{"Angelay", "Xiao Ming"}};
        List<List<String>> girlsList = new ArrayList<List<String>>();
        for(int i=0; i<beautyBoyArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<beautyBoyArray[i].length; j++) {
                columnList.add(beautyBoyArray[i][j]);
            }
            girlsList.add(columnList);
        }
        return girlsList;
    }

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库，创建表格和插入数据")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(TableInnerJoin.connection);
        tableJoinObj.createInnerTables();
        tableJoinObj.insertDataToInnerTables();

        String departmentTablepath = "src/test/resources/testdata/tableInsertValues/departments.txt";
        String employeeTablepath = "src/test/resources/testdata/tableInsertValues/employees.txt";
        String depValues = FileReaderUtil.readFile(departmentTablepath);
        String empValues = FileReaderUtil.readFile(employeeTablepath);
        tableJoinObj.createEmployeesTables();
        tableJoinObj.insertValuesToEmployeeTables(depValues, empValues);

        String selfJoinTablepath = "src/test/resources/testdata/tableInsertValues/selfjoin.txt";
        String selfJoinValues = FileReaderUtil.readFile(selfJoinTablepath);
        tableJoinObj.createSelfJoinTable();
        tableJoinObj.insertValuesToSelftJoinTable(selfJoinValues);
    }

    @Test(priority = 0, enabled = true, description = "验证等值连接查询各自特有字段不使用表名修饰")
    public void test01InnerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        List<List<String>> expectedInnerJoinList = girlsJoinList();
        System.out.println("Expected: " + expectedInnerJoinList);
        List<List<String>> actualInnerJoinList = tableJoinObj.innerJoinOwnFieldWithoutTablePrefix();
        System.out.println("Acutal: " + actualInnerJoinList);

        Assert.assertTrue(actualInnerJoinList.containsAll(expectedInnerJoinList));
        Assert.assertTrue(expectedInnerJoinList.containsAll(actualInnerJoinList));

//        for(int i=0; i<expectedInnerJoinList.size(); i++) {
//            Assert.assertTrue(actualInnerJoinList.contains(expectedInnerJoinList.get(i)));
//        }
//        for(int j=0; j<actualInnerJoinList.size(); j++) {
//            Assert.assertTrue(expectedInnerJoinList.contains(actualInnerJoinList.get(j)));
//        }
    }

    @Test(priority = 1, enabled = true, description = "验证表名调换")
    public void test02InnerJoinTableExchange() throws SQLException {
        List<List<String>> expectedExchangeList = girlsJoinList();
        System.out.println("Expected: " + expectedExchangeList);
        List<List<String>> actualExchangeList = tableJoinObj.innerJoinTableExchange();
        System.out.println("Acutal: " + actualExchangeList);

        Assert.assertTrue(actualExchangeList.containsAll(expectedExchangeList));
        Assert.assertTrue(expectedExchangeList.containsAll(actualExchangeList));

    }

    @Test(priority = 2, enabled = true, description = "验证省略inner")
    public void test03JoinOmitInner() throws SQLException {
        List<List<String>> expectedOmitInnerList = girlsJoinList();
        System.out.println("Expected: " + expectedOmitInnerList);
        List<List<String>> actualJoinOmitInnerList = tableJoinObj.joinOmitInner();
        System.out.println("Acutal: " + actualJoinOmitInnerList);

        Assert.assertTrue(actualJoinOmitInnerList.containsAll(expectedOmitInnerList));
        Assert.assertTrue(expectedOmitInnerList.containsAll(actualJoinOmitInnerList));
    }

    @Test(priority = 3, enabled = true, description = "验证等值连接使用别名")
    public void test04InnerJoinWithTableAlias() throws SQLException {
        List<List<String>> expectedInnerJoinWithTableAliasList = girlsJoinList();
        System.out.println("Expected: " + expectedInnerJoinWithTableAliasList);
        List<List<String>> actualInnerJoinWithTableAliasList = tableJoinObj.innerJoinWithTableAlias();
        System.out.println("Acutal: " + actualInnerJoinWithTableAliasList);

        Assert.assertTrue(actualInnerJoinWithTableAliasList.containsAll(expectedInnerJoinWithTableAliasList));
        Assert.assertTrue(expectedInnerJoinWithTableAliasList.containsAll(actualInnerJoinWithTableAliasList));
    }

    @Test(priority = 4, enabled = true, expectedExceptions = SQLException.class, description = "验证缺少等值条件，预期异常")
    public void test05InnerJoinWithoutCondition() throws SQLException {
        Statement noConditionStatement = TableInnerJoin.connection.createStatement();
        String innerJoinWithoutConditionSQL = "select name, boyname from beauty inner join boys";
        noConditionStatement.executeQuery(innerJoinWithoutConditionSQL);
        noConditionStatement.close();
    }

    @Test(priority = 5, enabled = true, expectedExceptions = SQLException.class, description = "验证起别名后等值条件依然使用表原名，预期异常")
    public void test06InnerJoinAliasWithOriginalName() throws SQLException {
        Statement originalStatement = TableInnerJoin.connection.createStatement();
        String innerJoinAliasWithOriginalNameSQL = "select g.id, g.name, b.boyname from beauty as g inner join boys as b on beauty.boyfriend_id = boys.id";
        originalStatement.executeQuery(innerJoinAliasWithOriginalNameSQL);
        originalStatement.close();
    }

    @Test(priority = 6, enabled = true, description = "验证查询两表的相同字段名必须使用表名修饰")
    public void test07InnerJoinSameFieldNameWithTablePrefix() throws SQLException {
        String[][] beautyBoyArray = {{"4", "ReBa", "2", "Han Han"},{"6", "zhiRuo", "1", "Zhang Wuji"}, {"8", "Xiao Zhao", "1", "Zhang Wuji"},
                {"10", "Wang Yuyan", "4", "DuanYU"}, {"12", "Zhao Min", "1", "Zhang Wuji"},{"3", "Angelay", "3", "Xiao Ming"}};
        List<List<String>> expectedGirlsList = new ArrayList<List<String>>();
        for(int i=0; i<beautyBoyArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<beautyBoyArray[i].length; j++) {
                columnList.add(beautyBoyArray[i][j]);
            }
            expectedGirlsList.add(columnList);
        }

        System.out.println("Expected: " + expectedGirlsList);
        List<List<String>> actualGirlsList = tableJoinObj.innerJoinSameFieldWithTablePrefix();
        System.out.println("Actual: " + actualGirlsList);

        Assert.assertTrue(actualGirlsList.containsAll(expectedGirlsList));
        Assert.assertTrue(expectedGirlsList.containsAll(actualGirlsList));
    }

    @Test(priority = 7, enabled = true, expectedExceptions = SQLException.class, description = "验证查询两表的相同字段名不使用表名修饰，预期异常")
    public void test08InnerJoinSameFieldNameWithoutTablePrefix() throws SQLException {
        Statement statementNoPrefix = TableInnerJoin.connection.createStatement();
        String innerJoinWithoutTablePrefixSQL = "select id, name, boyname from beauty inner join boys on beauty.boyfriend_id = boys.id";
        statementNoPrefix.executeQuery(innerJoinWithoutTablePrefixSQL);
        statementNoPrefix.close();
    }

    @Test(priority = 8, enabled = true, description = "验证使用group子句")
    public void test09InnerJoinGroupState() throws SQLException {
        String[][] groupArray = {{"1", "DuanYU"},{"1", "Xiao Ming"}, {"1", "Han Han"}, {"3", "Zhang Wuji"}};
        List<List<String>> expectedGroupList = new ArrayList<List<String>>();
        for(int i=0; i<groupArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<groupArray[i].length; j++) {
                columnList.add(groupArray[i][j]);
            }
            expectedGroupList.add(columnList);
        }

        System.out.println("Expected: " + expectedGroupList);
        List<List<String>> actualGroupList = tableJoinObj.innerJoinGroupState();
        System.out.println("Actual: " + actualGroupList);

        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(priority = 9, enabled = true, description = "验证使用Where条件子句")
    public void test10InnerJoinWhereState() throws SQLException {
        String[][] whereArray = {{"zhiRuo", "Zhang Wuji"},{"Xiao Zhao", "Zhang Wuji"}, {"ReBa", "Han Han"}, {"Angelay", "Xiao Ming"}};
        List<List<String>> expectedWhereList = new ArrayList<List<String>>();
        for(int i=0; i<whereArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<whereArray[i].length; j++) {
                columnList.add(whereArray[i][j]);
            }
            expectedWhereList.add(columnList);
        }

        System.out.println("Expected: " + expectedWhereList);
        List<List<String>> actualWhereList = tableJoinObj.innerJoinWhereState();
        System.out.println("Actual: " + actualWhereList);

        Assert.assertTrue(actualWhereList.containsAll(expectedWhereList));
        Assert.assertTrue(expectedWhereList.containsAll(actualWhereList));
    }

    @Test(priority = 10, enabled = true, description = "验证内等连接添加分组后排序")
    public void test11InnerJoinGroupAndOrder() throws SQLException {
        String[][] groupAndOrderArray = {{"45", "Shi"},{"35", "Sal"}, {"6", "Fin"}, {"6", "Pur"}, {"5","IT"},
                {"3","Exe"}, {"2","Acc"}, {"2","Mar"}, {"1","Adm"}, {"1","Pub"}, {"1","Hum"}};
        List<List<String>> expectedGroupAndOrderList = new ArrayList<List<String>>();
        for(int i=0; i<groupAndOrderArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<groupAndOrderArray[i].length; j++) {
                columnList.add(groupAndOrderArray[i][j]);
            }
            expectedGroupAndOrderList.add(columnList);
        }

        System.out.println("Expected: " + expectedGroupAndOrderList);

        List<List<String>> actualGroupAndOrderList = tableJoinObj.innerJoinGroupAndOrder();
        System.out.println("Actual: " + actualGroupAndOrderList);

        Assert.assertTrue(actualGroupAndOrderList.containsAll(expectedGroupAndOrderList));
        Assert.assertTrue(expectedGroupAndOrderList.containsAll(actualGroupAndOrderList));
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test11InnerJoinGroupAndOrder"},
            description = "验证内等连接添加分组后排序并限制条数")
    public void test12InnerJoinGroupAndOrderLimit() throws SQLException {
        String[][] groupAndOrderLimitArray = {{"1","Adm"}, {"1","Pub"}, {"1","Hum"}, {"2","Acc"}, {"2","Mar"}};
        List<List<String>> expectedGroupAndOrderLimitList = new ArrayList<List<String>>();
        for(int i=0; i<groupAndOrderLimitArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<groupAndOrderLimitArray[i].length; j++) {
                columnList.add(groupAndOrderLimitArray[i][j]);
            }
            expectedGroupAndOrderLimitList.add(columnList);
        }

        System.out.println("Expected: " + expectedGroupAndOrderLimitList);

        List<List<String>> actualGroupAndOrderLimitList = tableJoinObj.innerJoinGroupAndOrderLimit();
        System.out.println("Actual: " + actualGroupAndOrderLimitList);

        Assert.assertTrue(actualGroupAndOrderLimitList.containsAll(expectedGroupAndOrderLimitList));
        Assert.assertTrue(expectedGroupAndOrderLimitList.containsAll(actualGroupAndOrderLimitList));
    }

    @Test(priority = 12, enabled = true, expectedExceptions = SQLException.class, description = "验证查询两表的相同字段名不使用表名修饰，预期异常")
    public void test13InnerJoinWrongFieldCondition() throws SQLException {
        Statement wrongFieldStatement = TableInnerJoin.connection.createStatement();
        String innerJoinWrongFieldConditionSQL = "select name, boyname from beauty join boys on boys.id=beauty.boyfriendid";
        wrongFieldStatement.executeQuery(innerJoinWrongFieldConditionSQL);
        wrongFieldStatement.close();
    }

    @Test(priority = 13, enabled = true, description = "验证等值条件无相同数据的查询")
    public void test14InnerJoinNoSameData() throws SQLException {
        List<String> actualInnerJoinNoSameDataList = tableJoinObj.innerJoinNoSameData();
        Assert.assertTrue(actualInnerJoinNoSameDataList.isEmpty());

    }

    @Test(priority = 14, enabled = true, description = "验证自连接")
    public void test15selfJoin() throws SQLException {
        String[][] selfJoinArray = {{"Russell","Russell"},{"Bell","Russel"},{"Nayer","Bell"},{"Abel","Bell"},
                {"Kula","Russell"},{"Grant","Kula"},{"Sisi","Nayer"},{"Jason","Grant"},{"Peter","Abel"},
                {"School","Kula"},{"Hall","Abel"}};
        List<List<String>> expectedSelfJoinList = new ArrayList<List<String>>();
        for(int i=0; i<selfJoinArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<selfJoinArray[i].length; j++) {
                columnList.add(selfJoinArray[i][j]);
            }
            expectedSelfJoinList.add(columnList);
        }

        System.out.println("Expected: " + expectedSelfJoinList);
        List<List<String>> actualSelfJoinList = tableJoinObj.selfJoin();
        System.out.println("Actual: " + actualSelfJoinList);

        Assert.assertTrue(actualSelfJoinList.containsAll(expectedSelfJoinList));
        Assert.assertTrue(expectedSelfJoinList.containsAll(actualSelfJoinList));
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = TableInnerJoin.connection.createStatement();
        tearDownStatement.execute("delete from beauty");
        tearDownStatement.execute("drop table beauty");
        tearDownStatement.execute("delete from boys");
        tearDownStatement.execute("drop table boys");
        tearDownStatement.execute("delete from mytest");
        tearDownStatement.execute("drop table mytest");
        tearDownStatement.execute("delete from departments");
        tearDownStatement.execute("drop table departments");
        tearDownStatement.execute("delete from employees");
        tearDownStatement.execute("drop table employees");

        tearDownStatement.close();
        TableInnerJoin.connection.close();
    }
}
