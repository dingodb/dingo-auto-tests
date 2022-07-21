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

    public void initBeautyTB() throws SQLException {
        String beauty_meta_path = "src/test/resources/testdata/tablemeta/beauty_tbl_meta.txt";
        String boys_meta_path = "src/test/resources/testdata/tablemeta/boys_tbl_meta.txt";
        String beauty_value_path = "src/test/resources/testdata/tableInsertValues/beauty_tbl.txt";
        String boys_value_path = "src/test/resources/testdata/tableInsertValues/boys_tbl.txt";
        String beauty_meta = FileReaderUtil.readFile(beauty_meta_path);
        String boys_meta = FileReaderUtil.readFile(boys_meta_path);
        String beauty_value = FileReaderUtil.readFile(beauty_value_path);
        String boys_value = FileReaderUtil.readFile(boys_value_path);

        tableJoinObj.createInnerTables(beauty_meta, boys_meta);
        tableJoinObj.insertDataToInnerTables(beauty_value, boys_value);
    }

    public void initEmployeeTB() throws SQLException {
        String departmentTablepath = "src/test/resources/testdata/tableInsertValues/departments.txt";
        String employeeTablepath = "src/test/resources/testdata/tableInsertValues/employees.txt";
        String depValues = FileReaderUtil.readFile(departmentTablepath);
        String empValues = FileReaderUtil.readFile(employeeTablepath);
        tableJoinObj.createEmployeesTables();
        tableJoinObj.insertValuesToEmployeeTables(depValues, empValues);
    }

    public void initSelfJoinTB() throws SQLException {
        String selfJoinTablepath = "src/test/resources/testdata/tableInsertValues/selfjoin.txt";
        String selfJoinValues = FileReaderUtil.readFile(selfJoinTablepath);
        tableJoinObj.createSelfJoinTable();
        tableJoinObj.insertValuesToSelftJoinTable(selfJoinValues);
    }


    public void initTable1054() throws SQLException {
        String table1054_1_meta_path = "src/test/resources/testdata/tablemeta/table1054_1_meta.txt";
        String table1054_1_value_path = "src/test/resources/testdata/tableInsertValues/table1054_1.txt";
        String table1054_1_meta = FileReaderUtil.readFile(table1054_1_meta_path);
        String table1054_1_value = FileReaderUtil.readFile(table1054_1_value_path);
        tableJoinObj.createTable1054_1(table1054_1_meta);
        tableJoinObj.insertValuesToTable1054_1(table1054_1_value);
        String table1054_2_meta_path = "src/test/resources/testdata/tablemeta/table1054_2_meta.txt";
        String table1054_2_value_path = "src/test/resources/testdata/tableInsertValues/table1054_2.txt";
        String table1054_2_meta = FileReaderUtil.readFile(table1054_2_meta_path);
        String table1054_2_value = FileReaderUtil.readFile(table1054_2_value_path);
        tableJoinObj.createTable1054_2(table1054_2_meta);
        tableJoinObj.insertValuesToTable1054_2(table1054_2_value);
    }

    public void initJobGradesTB() throws SQLException {
        String job_grades_meta_path = "src/test/resources/testdata/tablemeta/job_grades_meta.txt";
        String job_grades_value_path = "src/test/resources/testdata/tableInsertValues/job_grades.txt";
        String job_grades_meta = FileReaderUtil.readFile(job_grades_meta_path);
        String job_grades_value = FileReaderUtil.readFile(job_grades_value_path);
        tableJoinObj.createJobGradesTable(job_grades_meta);
        tableJoinObj.insertValuesToJobGrades(job_grades_value);
    }

    public static List<List> girlsJoinList() {
        String[][] beautyBoyArray = {{"ReBa","Han Han"},{"zhiRuo","Zhang Wuji"}, {"Xiao Zhao", "Zhang Wuji"},
                {"Wang Yuyan", "DuanYU"}, {"Zhao Min", "Zhang Wuji"},{"Angelay", "Xiao Ming"}};
        List<List> girlsList = new ArrayList<List>();
        for(int i=0; i<beautyBoyArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<beautyBoyArray[i].length; j++) {
                columnList.add(beautyBoyArray[i][j]);
            }
            girlsList.add(columnList);
        }
        return girlsList;
    }

    public static List<List> expectedNEJoinList1() {
        String[][] dataArray = {{"24000.0","E"},{"17000.0","E"},{"17000.0","E"},{"9000.0","C"},
                {"6000.0","C"},{"4800.0","B"},{"4800.0","B"},{"4200.0","B"},{"12000.0","D"},{"9000.0","C"},
                {"8200.0","C"},{"7700.0","C"},{"7800.0","C"},{"6900.0","C"},{"11000.0","D"},{"3100.0","B"},
                {"2900.0","A"},{"2800.0","A"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList2() {
        String[][] dataArray = {{"3","E"},{"38","C"},{"26","B"},{"16","D"},{"24","A"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList3() {
        String[][] dataArray = {{"3","E"},{"16","D"},{"38","C"},{"26","B"},{"24","A"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList4() {
        String[][] dataArray = {{"1","zhangsan","18","3","hello","35"},{"1","zhangsan","18","5","wangwu","25"},
                {"1","zhangsan","18","6","Haha","20"},{"2","lisi","20","3","hello","35"},
                {"2","lisi","20","5","wangwu","25"},{"3","wangwu","25","3","hello","35"},
                {"4","liuming","18","3","hello","35"},{"4","liuming","18","5","wangwu","25"},
                {"4","liuming","18","6","Haha","20"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList5() {
        String[][] dataArray = {{"2","lisi","20","1","zhangsan","18"}, {"3","wangwu","25","1","zhangsan","18"},
                {"3","wangwu","25","6","Haha","20"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList6() {
        String[][] dataArray = {{"1","zhangsan","18","3","hello","35"},{"1","zhangsan","18","5","wangwu","25"},
                {"1","zhangsan","18","6","Haha","20"},{"2","lisi","20","1","zhangsan","18"},
                {"2","lisi","20","3","hello","35"}, {"2","lisi","20","5","wangwu","25"},
                {"3","wangwu","25","1","zhangsan","18"},{"3","wangwu","25","3","hello","35"},
                {"3","wangwu","25","6","Haha","20"}, {"4","liuming","18","3","hello","35"},
                {"4","liuming","18","5","wangwu","25"}, {"4","liuming","18","6","Haha","20"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList7() {
        String[][] dataArray = {{"9","Shuange","9","1","Zhang Wuji"},{"9","Shuange","9","2","Han Han"},
                {"9","Shuange","9","3","Xiao Ming"},{"9","Shuange","9","4","DuanYU"},
                {"10","Wang Yuyan","4","1","Zhang Wuji"},{"10","Wang Yuyan","4","2","Han Han"},
                {"10","Wang Yuyan","4","3","Xiao Ming"},{"11","Xia Xue","9","1","Zhang Wuji"},
                {"11","Xia Xue","9","2","Han Han"},{"11","Xia Xue","9","3","Xiao Ming"},
                {"11","Xia Xue","9","4","DuanYU"},{"12","Zhao Min","1","2","Han Han"},
                {"12","Zhao Min","1","3","Xiao Ming"},{"12","Zhao Min","1","4","DuanYU"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List<List> expectedNEJoinList8() {
        String[][] dataArray = {{"1","2","3","3","1"},{"1","2","3","6","2"}, {"4","5","6","6","2"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }



    @BeforeClass(alwaysRun = true, description = "测试前连接数据库，创建表格和插入数据")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(TableInnerJoin.connection);
    }

    @Test(description = "创建内连接测试用表beauty和boys，并插入数据")
    public void test00createInnerTableAndInsertValues1() throws SQLException {
        initBeautyTB();
    }

    @Test(description = "创建employees和department表，并插入数据")
    public void test00createInnerTableAndInsertValues2() throws SQLException {
        initEmployeeTB();
    }

    @Test(description = "创建job_grades表，并插入数据")
    public void test00createInnerTableAndInsertValues3() throws SQLException {
        initJobGradesTB();
    }

    @Test(description = "创建用于自连接测试的表，并插入数据")
    public void test00createInnerTableAndInsertValues4() throws SQLException {
        initSelfJoinTB();
    }

    @Test(description = "创建用于内等-自然连接测试的表，并插入数据")
    public void test00createInnerTableAndInsertValues5() throws SQLException {
        initTable1054();
    }

    @Test(description = "创建用于内等连接，某一表无数据")
    public void test00createInnerTableAndInsertValues6() throws SQLException {
        tableJoinObj.createTable1069();
    }

    @Test(description = "创建表，有相同字段")
    public void test00createInnerTableAndInsertValues7() throws SQLException {
        tableJoinObj.createTable1174();
    }

    @Test(description = "创建用于非等值连接的测试表，并插入数据")
    public void test00createInnerTableAndInsertValues8() throws SQLException {
        tableJoinObj.createTable1059();
    }

    @Test(priority = 0, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证等值连接查询各自特有字段不使用表名修饰")
    public void test01InnerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        List<List> expectedInnerJoinList = girlsJoinList();
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

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"}, description = "验证表名调换")
    public void test02InnerJoinTableExchange() throws SQLException {
        List<List> expectedExchangeList = girlsJoinList();
        System.out.println("Expected: " + expectedExchangeList);
        List<List<String>> actualExchangeList = tableJoinObj.innerJoinTableExchange();
        System.out.println("Acutal: " + actualExchangeList);

        Assert.assertTrue(actualExchangeList.containsAll(expectedExchangeList));
        Assert.assertTrue(expectedExchangeList.containsAll(actualExchangeList));

    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"}, description = "验证省略inner")
    public void test03JoinOmitInner() throws SQLException {
        List<List> expectedOmitInnerList = girlsJoinList();
        System.out.println("Expected: " + expectedOmitInnerList);
        List<List<String>> actualJoinOmitInnerList = tableJoinObj.joinOmitInner();
        System.out.println("Acutal: " + actualJoinOmitInnerList);

        Assert.assertTrue(actualJoinOmitInnerList.containsAll(expectedOmitInnerList));
        Assert.assertTrue(expectedOmitInnerList.containsAll(actualJoinOmitInnerList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"}, description = "验证等值连接使用别名")
    public void test04InnerJoinWithTableAlias() throws SQLException {
        List<List> expectedInnerJoinWithTableAliasList = girlsJoinList();
        System.out.println("Expected: " + expectedInnerJoinWithTableAliasList);
        List<List<String>> actualInnerJoinWithTableAliasList = tableJoinObj.innerJoinWithTableAlias();
        System.out.println("Acutal: " + actualInnerJoinWithTableAliasList);

        Assert.assertTrue(actualInnerJoinWithTableAliasList.containsAll(expectedInnerJoinWithTableAliasList));
        Assert.assertTrue(expectedInnerJoinWithTableAliasList.containsAll(actualInnerJoinWithTableAliasList));
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            expectedExceptions = SQLException.class, description = "验证缺少等值条件，预期异常")
    public void test05InnerJoinWithoutCondition() throws SQLException {
        try(Statement noConditionStatement = TableInnerJoin.connection.createStatement()) {
            String innerJoinWithoutConditionSQL = "select name, boyname from beauty inner join boys";
            noConditionStatement.executeQuery(innerJoinWithoutConditionSQL);
        }
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            expectedExceptions = SQLException.class, description = "验证起别名后等值条件依然使用表原名，预期异常")
    public void test06InnerJoinAliasWithOriginalName() throws SQLException {
        try(Statement originalStatement = TableInnerJoin.connection.createStatement()) {
            String innerJoinAliasWithOriginalNameSQL = "select g.id, g.name, b.boyname from beauty as g inner join boys as b on beauty.boyfriend_id = boys.id";
            originalStatement.executeQuery(innerJoinAliasWithOriginalNameSQL);
        }
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证查询两表的相同字段名必须使用表名修饰")
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

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            expectedExceptions = SQLException.class, description = "验证查询两表的相同字段名不使用表名修饰，预期异常")
    public void test08InnerJoinSameFieldNameWithoutTablePrefix() throws SQLException {
        try(Statement statementNoPrefix = TableInnerJoin.connection.createStatement()) {
            String innerJoinWithoutTablePrefixSQL = "select id, name, boyname from beauty inner join boys on beauty.boyfriend_id = boys.id";
            statementNoPrefix.executeQuery(innerJoinWithoutTablePrefixSQL);
        }
    }

    @Test(priority = 8, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证使用group子句")
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

    @Test(priority = 9, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证使用Where条件子句")
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

    @Test(priority = 10, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues2"},
            description = "验证内等连接添加分组后排序")
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

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues2"},
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

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            expectedExceptions = SQLException.class, description = "验证查询两表的相同字段名不使用表名修饰，预期异常")
    public void test13InnerJoinWrongFieldCondition() throws SQLException {
        try(Statement wrongFieldStatement = TableInnerJoin.connection.createStatement()) {
            String innerJoinWrongFieldConditionSQL = "select name, boyname from beauty join boys on boys.id=beauty.boyfriendid";
            wrongFieldStatement.executeQuery(innerJoinWrongFieldConditionSQL);
        }
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证等值条件无相同数据的查询")
    public void test14InnerJoinNoSameData() throws SQLException {
        List<String> actualInnerJoinNoSameDataList = tableJoinObj.innerJoinNoSameData();
        Assert.assertTrue(actualInnerJoinNoSameDataList.isEmpty());

    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues4"},
            description = "验证自连接")
    public void test15selfJoin() throws SQLException {
        String[][] selfJoinArray = {{"Russell","Russell"},{"Bell","Russell"},{"Nayer","Bell"},{"Abel","Bell"},
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


    @Test(priority = 15, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues5"},
            description = "验证内等连接-自然连接")
    public void test16NaturalJoin() throws SQLException {
        String[][] joinArray = {{"9002","Liu Chen","Female","IS","2","90"}, {"9002","Liu Chen","Female","IS","3","80"},
                {"9001","Li Yong","Male","CS","1","92"}, {"9001","Li Yong","Male","CS","2","85"},
                {"9001","Li Yong","Male","CS","3","88"}};
        List<List> expectedJoinList = new ArrayList<List>();
        for(int i=0; i<joinArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<joinArray[i].length; j++) {
                columnList.add(joinArray[i][j]);
            }
            expectedJoinList.add(columnList);
        }
        System.out.println("Expected: " + expectedJoinList);
        List<List> actualJoinList = tableJoinObj.naturalJoin();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"},
            description = "验证内等连接使用逗号分隔表名，和where等值条件筛选")
    public void test17InnerJoinCommaWithWhere() throws SQLException {
        String[][] joinArray = {{"Angelay","Xiao Ming"},{"ReBa","Han Han"},{"zhiRuo","Zhang Wuji"},
                {"Xiao Zhao","Zhang Wuji"},{"Wang Yuyan","DuanYU"},{"Zhao Min","Zhang Wuji"}};
        List<List> expectedJoinList = new ArrayList<List>();
        for(int i=0; i<joinArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<joinArray[i].length; j++) {
                columnList.add(joinArray[i][j]);
            }
            expectedJoinList.add(columnList);
        }
        System.out.println("Expected: " + expectedJoinList);
        List<List> actualJoinList = tableJoinObj.innerJoinCommaWhere();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }


    @Test(priority = 17, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues6"},
            description = "验证某一表无数据返回空")
    public void test18InnerJoinOneEmpty() throws SQLException {
        Boolean actualJoinResult = tableJoinObj.innerJoinOneEmpty();
        Assert.assertFalse(actualJoinResult);
    }

    @Test(priority = 18, enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues7"},
            description = "验证内等连接using(key)")
    public void test19InnerJoinUsingKey() throws SQLException {
        String[][] joinArray = {{"1","zhangsan","18","1","zhangsan","18"}, {"3","wangwu","25","3","hello","35"}};
        List<List> expectedJoinList = new ArrayList<List>();
        for(int i=0; i<joinArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<joinArray[i].length; j++) {
                columnList.add(joinArray[i][j]);
            }
            expectedJoinList.add(columnList);
        }
        System.out.println("Expected: " + expectedJoinList);
        List<List> actualJoinList = tableJoinObj.innerJoinUsingKey();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues2", "test00createInnerTableAndInsertValues3"},
            description = "内非连接，多个条件")
    public void test20InnerNEJoinMultiCondition() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList1();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinMultiCondition();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues2", "test00createInnerTableAndInsertValues3"},
            description = "内非连接添加分组")
    public void test21InnerNEJoinWithGroup() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList2();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinWithGroup();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues2", "test00createInnerTableAndInsertValues3"},
            description = "内非连接添加排序")
    public void test22InnerNEJoinWithOrder() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList3();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinWithOrder();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertEquals(actualJoinList,expectedJoinList);
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues7"}, description = "内非连接小于")
    public void test23InnerNEJoinST() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList4();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinST();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues7"}, description = "内非连接大于")
    public void test24InnerNEJoinLT() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList5();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinLT();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues7"}, description = "内非连接不等于")
    public void test25InnerNEJoinNE() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList6();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinNE();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues8"}, description = "内非连接无符合条件数据返回空")
    public void test26InnerNEJoinNoDataMeet() throws SQLException {
        Boolean actualQueryResult = tableJoinObj.innerNEJoinNoDataQuery();
        System.out.println("Actual: " + actualQueryResult);
        Assert.assertFalse(actualQueryResult);
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues8"}, expectedExceptions = SQLException.class,
            description = "内非连接字段不存在，预期异常")
    public void test27InnerNEJoinFieldNotExist() throws SQLException {
        tableJoinObj.innerNEJoinNFieldNotExist();
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues1"}, description = "内非连接添加Where")
    public void test28InnerNEJoinWithWhere() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList7();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList = tableJoinObj.innerNEJoinWithWhere();
        System.out.println("Actual: " + actualJoinList);
        Assert.assertTrue(actualJoinList.containsAll(expectedJoinList));
        Assert.assertTrue(expectedJoinList.containsAll(actualJoinList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues8"}, description = "内非连接无符合条件数据返回空")
    public void test29InnerNEJoinOneEmpty1() throws SQLException {
        Boolean actualQueryResult = tableJoinObj.innerNEJoinOneEmpty_1();
        System.out.println("Actual: " + actualQueryResult);
        Assert.assertFalse(actualQueryResult);
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues8"}, description = "内非连接无符合条件数据返回空")
    public void test29InnerNEJoinOneEmpty2() throws SQLException {
        Boolean actualQueryResult = tableJoinObj.innerNEJoinOneEmpty_2();
        System.out.println("Actual: " + actualQueryResult);
        Assert.assertFalse(actualQueryResult);
    }

    @Test(enabled = true, dependsOnMethods = {"test00createInnerTableAndInsertValues8"}, description = "内非连接两表互换")
    public void test30InnerNEJoinTableExchange() throws SQLException {
        List<List> expectedJoinList = expectedNEJoinList8();
        System.out.println("Expected: " + expectedJoinList);

        List<List> actualJoinList1 = tableJoinObj.innerNEJoinExchangeTable1();
        List<List> actualJoinList2 = tableJoinObj.innerNEJoinExchangeTable2();
        System.out.println("Actual: " + actualJoinList1);
        System.out.println("Actual: " + actualJoinList2);
        Assert.assertEquals(actualJoinList1, expectedJoinList);
        Assert.assertEquals(actualJoinList2, expectedJoinList);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
//        tearDownStatement = TableInnerJoin.connection.createStatement();
//        tearDownStatement.execute("delete from beauty");
//        tearDownStatement.execute("drop table beauty");
//        tearDownStatement.execute("delete from boys");
//        tearDownStatement.execute("drop table boys");
//        tearDownStatement.execute("delete from mytest");
//        tearDownStatement.execute("drop table mytest");
//        tearDownStatement.execute("delete from departments");
//        tearDownStatement.execute("drop table departments");
//        tearDownStatement.execute("delete from employees");
//        tearDownStatement.execute("drop table employees");
//        tearDownStatement.execute("delete from table1054_1");
//        tearDownStatement.execute("drop table table1054_1");
//        tearDownStatement.execute("delete from table1054_2");
//        tearDownStatement.execute("drop table table1054_2");
//        tearDownStatement.execute("delete from table1069_1");
//        tearDownStatement.execute("drop table table1069_1");
//        tearDownStatement.execute("delete from table1069_2");
//        tearDownStatement.execute("drop table table1069_2");
//        tearDownStatement.execute("delete from table1174_1");
//        tearDownStatement.execute("drop table table1174_1");
//        tearDownStatement.execute("delete from table1174_2");
//        tearDownStatement.execute("drop table table1174_2");
//        tearDownStatement.execute("delete from job_grades");
//        tearDownStatement.execute("drop table job_grades");
//        tearDownStatement.execute("delete from table1059_1");
//        tearDownStatement.execute("drop table table1059_1");
//        tearDownStatement.execute("delete from table1059_2");
//        tearDownStatement.execute("drop table table1059_2");
//        tearDownStatement.execute("delete from table1059_3");
//        tearDownStatement.execute("drop table table1059_3");
//        tearDownStatement.execute("delete from table1059_4");
//        tearDownStatement.execute("drop table table1059_4");
//
//        tearDownStatement.close();
//        TableInnerJoin.connection.close();

        try {
            tearDownStatement = TableInnerJoin.connection.createStatement();
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
            tearDownStatement.execute("delete from table1054_1");
            tearDownStatement.execute("drop table table1054_1");
            tearDownStatement.execute("delete from table1054_2");
            tearDownStatement.execute("drop table table1054_2");
            tearDownStatement.execute("delete from table1069_1");
            tearDownStatement.execute("drop table table1069_1");
            tearDownStatement.execute("delete from table1069_2");
            tearDownStatement.execute("drop table table1069_2");
            tearDownStatement.execute("delete from table1174_1");
            tearDownStatement.execute("drop table table1174_1");
            tearDownStatement.execute("delete from table1174_2");
            tearDownStatement.execute("drop table table1174_2");
            tearDownStatement.execute("delete from job_grades");
            tearDownStatement.execute("drop table job_grades");
            tearDownStatement.execute("delete from table1059_1");
            tearDownStatement.execute("drop table table1059_1");
            tearDownStatement.execute("delete from table1059_2");
            tearDownStatement.execute("drop table table1059_2");
            tearDownStatement.execute("delete from table1059_3");
            tearDownStatement.execute("drop table table1059_3");
            tearDownStatement.execute("delete from table1059_4");
            tearDownStatement.execute("drop table table1059_4");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(tearDownStatement != null) {
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if(TableInnerJoin.connection != null) {
                    TableInnerJoin.connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
