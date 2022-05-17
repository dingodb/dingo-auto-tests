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

import io.dingodb.dailytest.TableOuterJoin;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestTableOuterJoin {
    public static TableOuterJoin outerJoinObj = new TableOuterJoin();

    public void initStudentAndClassTB() throws SQLException {
        String student_tbl_meta_path = "src/test/resources/testdata/tablemeta/student_tbl_meta.txt";
        String student_tbl_value_path = "src/test/resources/testdata/tableInsertValues/student_tbl.txt";
        String student_tbl_meta = FileReaderUtil.readFile(student_tbl_meta_path);
        String student_tbl_value = FileReaderUtil.readFile(student_tbl_value_path);
        outerJoinObj.createStudentTable(student_tbl_meta);
        outerJoinObj.insertValuesToStudent(student_tbl_value);
        outerJoinObj.createStudent1Table(student_tbl_meta);
        String class_tbl_meta_path = "src/test/resources/testdata/tablemeta/class_tbl_meta.txt";
        String class_tbl_value_path = "src/test/resources/testdata/tableInsertValues/class_tbl.txt";
        String class_tbl_meta = FileReaderUtil.readFile(class_tbl_meta_path);
        String class_tbl_value = FileReaderUtil.readFile(class_tbl_value_path);
        outerJoinObj.createClassTable(class_tbl_meta);
        outerJoinObj.insertValuesToClass(class_tbl_value);
        outerJoinObj.createClass1Table(class_tbl_meta);
    }

    public void initProductTB() throws SQLException {
        String product1_meta_path = "src/test/resources/testdata/tablemeta/product1_meta.txt";
        String product1_value_path = "src/test/resources/testdata/tableInsertValues/product1.txt";
        String product1_meta = FileReaderUtil.readFile(product1_meta_path);
        String product1_value = FileReaderUtil.readFile(product1_value_path);
        outerJoinObj.createProduct1Table(product1_meta);
        outerJoinObj.insertValuesToProduct1(product1_value);
        String product2_meta_path = "src/test/resources/testdata/tablemeta/product2_meta.txt";
        String product2_value_path = "src/test/resources/testdata/tableInsertValues/product2.txt";
        String product2_meta = FileReaderUtil.readFile(product2_meta_path);
        String product2_value = FileReaderUtil.readFile(product2_value_path);
        outerJoinObj.createProuct2Table(product2_meta);
        outerJoinObj.insertValuesToProduct2(product2_value);
    }

    public void initTestTB() throws SQLException {
        String test_meta_path = "src/test/resources/testdata/tablemeta/test_meta.txt";
        String test1_value_path = "src/test/resources/testdata/tableInsertValues/test1.txt";
        String test_meta = FileReaderUtil.readFile(test_meta_path);
        String test1_value = FileReaderUtil.readFile(test1_value_path);
        outerJoinObj.createTest1Table(test_meta);
        outerJoinObj.insertValuesToTest1(test1_value);
        String test2_value_path = "src/test/resources/testdata/tableInsertValues/test2.txt";
        String test2_value = FileReaderUtil.readFile(test2_value_path);
        outerJoinObj.createTest2Table(test_meta);
        outerJoinObj.insertValuesToTest2(test2_value);
    }

    public List<List> expectedList1() {
        String[][] dataArray = {{"2","lisi","101","101","class-2"},{"1","zhangsan","100","100","class-1"},
                {null,null,null,"103","class-3"},{"3","wangwu","102",null,null}};
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

    public List<List> expectedList2() {
        String[][] dataArray = {{"1","2","3",null,null},{"4","5","6",null,null},
                {"7","8","9",null,null},{null,null,null,"3","1"},{null,null,null,"6","2"}};
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

    public List<List> expectedList3() {
        String[][] dataArray = {{"2","lisi","101",null,null},{"1","zhangsan","100",null,null},
                {"3","wangwu","102",null,null}};
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

    public List<List> expectedList4() {
        String[][] dataArray = {{"2","Lisi",null,null},{null,null,"5","Hello"},{"1","Zhangsan","1","Zhangsan"},
                {"4","Tita",null,null},{"3","Wang Wu","3","Wang Wu"},{null,null,"6","NiNi"}};
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

    public List<List> expectedList5() {
        String[][] dataArray = {{"2","lisi","class-2"},{"1","zhangsan","class-1"},
                {null,null,"class-3"},{"3","wangwu",null}};
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

    public List<List> expectedList6() {
        String[][] dataArray = {{"2","lisi","101","101","class-2"},{"1","zhangsan","100","100","class-1"}};
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

    public List<List> expectedList7() {
        String[][] dataArray = {{null,null,null,"103","class-3"},{"3","wangwu","102",null,null}};
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

    public List<List> expectedCrossList1() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"},
                {"1","zhangsan","100","101","class-2"}, {"1","zhangsan","100","103","class-3"},
                {"2","lisi","101","100","class-1"}, {"2","lisi","101","101","class-2"},
                {"2","lisi","101","103","class-3"}, {"3","wangwu","102","100","class-1"},
                {"3","wangwu","102","101","class-2"}, {"3","wangwu","102","103","class-3"}};
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

    public List<List> expectedCrossList2() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"}, {"2","lisi","101","101","class-2"}};
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

    public List<List> expectedCrossList3() {
        String[][] dataArray = {{"1","Zhangsan","1","Zhangsan"}, {"1","Zhangsan","3","Wang Wu"},
                {"1","Zhangsan","5","Hello"},{"1","Zhangsan","6","NiNi"},
                {"2","Lisi","1","Zhangsan"}, {"2","Lisi","3","Wang Wu"},
                {"2","Lisi","5","Hello"},{"2","Lisi","6","NiNi"},
                {"3","Wang Wu","1","Zhangsan"}, {"3","Wang Wu","3","Wang Wu"},
                {"3","Wang Wu","5","Hello"},{"3","Wang Wu","6","NiNi"},
                {"4","Tita","1","Zhangsan"}, {"4","Tita","3","Wang Wu"},
                {"4","Tita","5","Hello"},{"4","Tita","6","NiNi"}};
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

    public List<List> expectedCrossList4() {
        String[][] dataArray = {{"1","zhangsan","class-1"},
                {"1","zhangsan","class-2"}, {"1","zhangsan","class-3"},
                {"2","lisi","class-1"}, {"2","lisi","class-2"},
                {"2","lisi","class-3"}, {"3","wangwu","class-1"},
                {"3","wangwu","class-2"}, {"3","wangwu","class-3"}};
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

    public List<List> expectedCrossList5() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"},
                {"2","lisi","101","100","class-1"}, {"3","wangwu","102","100","class-1"},
                {"1","zhangsan","100","101","class-2"}, {"2","lisi","101","101","class-2"},
                {"3","wangwu","102","101","class-2"}, {"1","zhangsan","100","103","class-3"},
                {"2","lisi","101","103","class-3"}, {"3","wangwu","102","103","class-3"},};
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

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(TableOuterJoin.connection);
    }

    @Test(priority = 0, enabled = true, description = "验证全外连接查询全部数据")
    public void test01FullOuterJoinAllData() throws SQLException {
        initStudentAndClassTB();
        List<List> expectedList = expectedList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListWithOuter = outerJoinObj.fullOuterJoinAll();
        System.out.println("Actual: " + actualListWithOuter);

        Assert.assertTrue(actualListWithOuter.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListWithOuter));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证省略outer")
    public void test02FullOuterJoinOmitOuter() throws SQLException {
        List<List> expectedList = expectedList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListOmitOuter = outerJoinObj.fullOuterJoinOmitOuter();
        System.out.println("Actual: " + actualListOmitOuter);

        Assert.assertTrue(actualListOmitOuter.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListOmitOuter));
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证缺少连接条件，执行预期失败")
    public void test03FullOuterJoinWithoutCondition() throws SQLException {
        outerJoinObj.fullOuterJoinWithoutCondition();
    }

    @Test(priority = 3, enabled = true, description = "验证两表没有相同数据时查询全连接数据")
    public void test04FullOuterJoinNoSameData() throws SQLException {
        initProductTB();
        List<List> expectedList = expectedList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListNoSameData = outerJoinObj.fullOuterJoinNoSameData();
        System.out.println("Actual: " + actualListNoSameData);

        Assert.assertTrue(actualListNoSameData.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListNoSameData));
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证当一个表为空时，全连接查询")
    public void test05FullOuterJoinOneEmpty() throws SQLException {
        List<List> expectedList = expectedList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualListOneEmpty = outerJoinObj.fullOuterJoinOneEmpty();
        System.out.println("Actual: " + actualListOneEmpty);

        Assert.assertTrue(actualListOneEmpty.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListOneEmpty));
    }

    @Test(priority = 5, enabled = true, description = "验证使用using(key)作为条件")
    public void test06FullOuterJoinUsingKeyState() throws SQLException {
        initTestTB();
        List<List> expectedList = expectedList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUsingKeyState = outerJoinObj.fullOuterJoinUsingKeyState();
        System.out.println("Actual: " + actualListUsingKeyState);

        Assert.assertTrue(actualListUsingKeyState.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUsingKeyState));
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test04FullOuterJoinNoSameData"},
            expectedExceptions = SQLException.class, description = "验证条件字段不存在，执行预期失败")
    public void test07FullOuterJoinWrongKey() throws SQLException {
        outerJoinObj.fullOuterJoinWrongKey();
    }

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test06FullOuterJoinUsingKeyState"},
            description = "验证使用*查询所有")
    public void test08FullOuterJoinStarQueryAll() throws SQLException {
        List<List> expectedList = expectedList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListStarQueryAll = outerJoinObj.fullOuterJoinStarQueryAll();
        System.out.println("Actual: " + actualListStarQueryAll);

        Assert.assertTrue(actualListStarQueryAll.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll));
    }

    @Test(priority = 8, enabled = true, dependsOnMethods = {"test06FullOuterJoinUsingKeyState"},
            expectedExceptions = SQLException.class, description = "验证查询相同字段，不使用表名修饰预期失败")
    public void test09FullOuterJoinWrongKey() throws SQLException {
        outerJoinObj.fullOuterJoinSameIDWithoutTablePrefix();
    }

    @Test(priority = 9, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询独有字段可不使用表名修饰")
    public void test10FullOuterJoinUniqueFieldQuery() throws SQLException {
        List<List> expectedList = expectedList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUniqueFieldQuery = outerJoinObj.fullOuterJoinUniqueFieldQuery();
        System.out.println("Actual: " + actualListUniqueFieldQuery);

        Assert.assertTrue(actualListUniqueFieldQuery.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUniqueFieldQuery));
    }

    @Test(priority = 10, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证表名位置互换查询结果相同")
    public void test11FullOuterJoinExchangeTable() throws SQLException {
        List<List> expectedList = expectedList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListExchangeTable = outerJoinObj.fullOuterJoinExchangeTable();
        System.out.println("Actual: " + actualListExchangeTable);

        Assert.assertTrue(actualListExchangeTable.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListExchangeTable));
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表匹配到的数据")
    public void test12FullOuterJoinMatchRows() throws SQLException {
        List<List> expectedList = expectedList6();
        System.out.println("Expected: " + expectedList);
        List<List> actualListMatchRow = outerJoinObj.fullOuterJoinMatchRow();
        System.out.println("Actual: " + actualListMatchRow);

        Assert.assertTrue(actualListMatchRow.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListMatchRow));
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表未匹配到的数据")
    public void test13FullOuterJoinNotMatchRows() throws SQLException {
        List<List> expectedList = expectedList7();
        System.out.println("Expected: " + expectedList);
        List<List> actualListNotMatchRow = outerJoinObj.fullOuterJoinNotMatchRow();
        System.out.println("Actual: " + actualListNotMatchRow);

        Assert.assertTrue(actualListNotMatchRow.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListNotMatchRow));
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证没有相同字段，使用using(key)条件预期失败")
    public void test14FullOuterJoinUsingKeyNoSame() throws SQLException {
        outerJoinObj.fullOuterJoinUsingKeyNoSameField();
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证两表均为空，全连接返回空，无异常")
    public void test15FullOuterJoinBothEmpty() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.fullOuterJoinBothEmpty();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表交叉连接的全部数据")
    public void test16CrossJoinAllData() throws SQLException {
        List<List> expectedList = expectedCrossList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCrossAll = outerJoinObj.crossJoinAll();
        System.out.println("Actual: " + actualListCrossAll);

        Assert.assertTrue(actualListCrossAll.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCrossAll));
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证使用逗号分隔做交叉连接")
    public void test17CrossJoinCommaSeparate() throws SQLException {
        List<List> expectedList = expectedCrossList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCrossComma = outerJoinObj.crossJoinAllSeprateComma();
        System.out.println("Actual: " + actualListCrossComma);

        Assert.assertTrue(actualListCrossComma.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCrossComma));
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证交叉连接不允许使用连接条件")
    public void test18CrossJoinExtraCondition() throws SQLException {
        outerJoinObj.crossJoinExtraCondition();
    }

    @Test(priority = 18, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证连接后使用where条件过滤")
    public void test19CrossJoinWhereCondition() throws SQLException {
        List<List> expectedList = expectedCrossList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListWhereCondition = outerJoinObj.crossJoinWhereCondition();
        System.out.println("Actual: " + actualListWhereCondition);

        Assert.assertTrue(actualListWhereCondition.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListWhereCondition));
    }

    @Test(priority = 19, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证使用逗号分隔交叉连接后使用where条件过滤")
    public void test20CrossJoinCommaWhereCondition() throws SQLException {
        List<List> expectedList = expectedCrossList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCommaWhereCondition = outerJoinObj.crossJoinCommaWhereCondition();
        System.out.println("Actual: " + actualListCommaWhereCondition);

        Assert.assertTrue(actualListCommaWhereCondition.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCommaWhereCondition));
    }

    @Test(priority = 20, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证一表为空，交叉连接返回空，无异常")
    public void test21CrossJoinOneEmpty() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.crossJoinOneEmpty();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 21, enabled = true, dependsOnMethods = {"test06FullOuterJoinUsingKeyState"},
            description = "验证使用*查询交叉连接全部数据")
    public void test22CrossJoinStarQueryAll() throws SQLException {
        List<List> expectedList = expectedCrossList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualListStarQueryAll1 = outerJoinObj.crossJoinStarQueryAll1();
        System.out.println("Actual: " + actualListStarQueryAll1);
        Assert.assertTrue(actualListStarQueryAll1.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll1));

        List<List> actualListStarQueryAll2 = outerJoinObj.crossJoinStarQueryAll2();
        System.out.println("Actual: " + actualListStarQueryAll2);
        Assert.assertTrue(actualListStarQueryAll2.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll2));
    }

    @Test(priority = 22, enabled = true, dependsOnMethods = {"test06FullOuterJoinUsingKeyState"},
            expectedExceptions = SQLException.class, description = "验证交叉连查询相同id不使用表名修饰，预期失败")
    public void test23CrossJoinSameFieldNoTablePrefix() throws SQLException {
        outerJoinObj.crossJoinSameFieldNoTablePrefix();
    }

    @Test(priority = 23, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表独有字段不需使用表名修饰")
    public void test24CrossJoinUniqueFieldNoTablePrefix() throws SQLException {
        List<List> expectedList = expectedCrossList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUniqueField = outerJoinObj.crossJoinUniqueFieldNoTablePrefix();
        System.out.println("Actual: " + actualListUniqueField);

        Assert.assertTrue(actualListUniqueField.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUniqueField));
    }

    @Test(priority = 24, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证调换位置做交叉连接，笛卡尔积结果不同")
    public void test25CrossJoinExchangeTable() throws SQLException {
        List<List> expectedList = expectedCrossList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualListExchangeTable = outerJoinObj.crossJoinExchangeTable();
        System.out.println("Actual: " + actualListExchangeTable);

        Assert.assertTrue(actualListExchangeTable.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListExchangeTable));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = TableOuterJoin.connection.createStatement();
        tearDownStatement.execute("delete from student_tbl");
        tearDownStatement.execute("drop table student_tbl");
        tearDownStatement.execute("delete from class_tbl");
        tearDownStatement.execute("drop table class_tbl");
        tearDownStatement.execute("delete from student_tbl1");
        tearDownStatement.execute("drop table student_tbl1");
        tearDownStatement.execute("delete from class_tbl1");
        tearDownStatement.execute("drop table class_tbl1");
        tearDownStatement.execute("delete from product1");
        tearDownStatement.execute("drop table product1");
        tearDownStatement.execute("delete from product2");
        tearDownStatement.execute("drop table product2");
        tearDownStatement.execute("delete from test1");
        tearDownStatement.execute("drop table test1");
        tearDownStatement.execute("delete from test2");
        tearDownStatement.execute("drop table test2");

        tearDownStatement.close();
        TableOuterJoin.connection.close();
    }
}
