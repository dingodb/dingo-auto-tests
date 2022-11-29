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

import io.dingodb.common.utils.JDBCUtils;
import io.dingodb.dailytest.SQLFuncs;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.StrTo2DList;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Listeners(EmailableReporterListener.class)
public class TestSQLFuncs extends YamlDataHelper {
    public static SQLFuncs funcObj = new SQLFuncs();

    public void initTable(String tableName, String tableMetaPath, String tableValuePath) throws SQLException {
        String queryTable1_meta = FileReaderUtil.readFile(tableMetaPath);
        String queryTable1_value = FileReaderUtil.readFile(tableValuePath);
        funcObj.queryTable1Create(tableName, queryTable1_meta);
        funcObj.queryTable1InsertValues(tableName, queryTable1_value);
    }

    public static List<List> expectedOutData(String[][] dataArray) {
        List<List> expectedList = new ArrayList<List>();
        for(int i = 0; i < dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j = 0; j < dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public static List expectedOutData(Object[] dataArray) {
        List expectedList = new ArrayList();
        for (int i=0; i < dataArray.length; i++){
            expectedList.add(dataArray[i]);
        }
        return expectedList;
    }


    @BeforeClass()
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        Assert.assertNotNull(SQLFuncs.connection);
    }

    @Test(description = "创建SQL功能测试表1")
    public void test00CreateFuncTable1() throws SQLException, ClassNotFoundException {
        funcObj.createFuncTable();
    }

    @Test(description = "创建SQL功能测试表2 - 空表")
    public void test00CreateFuncTable2() throws SQLException, ClassNotFoundException {
        funcObj.createEmpTable();
    }

    @Test(dependsOnGroups = {"emp"}, description = "空表插入一条数据")
    public void test00InsertSingleTable2() throws SQLException, ClassNotFoundException {
        funcObj.insertOneRowToTable();
    }

    @Test(dependsOnGroups = {"single"}, description = "在单条数据基础上再插入多条数据")
    public void test00InsertMultiTable2() throws SQLException, ClassNotFoundException {
        funcObj.insertMoreRowsToTable();
    }

    @Test(description = "创建SQL功能测试表3 - 用户插入null的验证")
    public void test00CreateFuncTable3() throws SQLException, ClassNotFoundException {
        funcObj.createTable302();
    }

    /**
     * 2022/8/11 - 查询测试用例补充 - 测试用表创建和插入数据
     * @throws SQLException
     */
    @Test(enabled = true, description = "创建查询测试表1")
    public void test00CreateQueryTable1() throws SQLException {
        String tableName = "querytest1";
        String queryTable1_meta_path = "src/test/resources/tabledata/meta/common/common_meta1.txt";
        String queryTable1_value_path = "src/test/resources/tabledata/value/sqlOperation/querytest1_value.txt";
        initTable(tableName, queryTable1_meta_path, queryTable1_value_path);
    }

    @Test(enabled = true, description = "创建查询测试表2")
    public void test00CreateGroupTable1() throws SQLException {
        String tableName = "grouptest1";
        String group1_meta_path = "src/test/resources/tabledata/meta/sqlOperation/group_tbl1_meta.txt";
        String group1_value_path = "src/test/resources/tabledata/value/sqlOperation/group_tbl1_value.txt";
        initTable(tableName, group1_meta_path, group1_value_path);
    }

    @Test(groups = {"preFuncs"}, dependsOnMethods = {"test00CreateFuncTable1"},
            description = "验证批量插入数据到创建的数据表中")
    public void test01MultiInsert() throws SQLException, ClassNotFoundException {
        int expectedMultiInsertCount = 9;
        int actualMultiInsertCount = funcObj.insertMultiValues();
        Assert.assertEquals(actualMultiInsertCount, expectedMultiInsertCount);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证表格数据统计")
    public void test02CountFunc() throws SQLException, ClassNotFoundException {
        int expectedCountRows = 9;
        System.out.println("Expected: " + expectedCountRows);
        int actualCountRows = funcObj.countFunc();
        System.out.println("实际统计总数：" + actualCountRows);

        Assert.assertEquals(actualCountRows, expectedCountRows);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按name字段去重查询结果正确")
    public void test03DistinctNameFunc() throws SQLException, ClassNotFoundException {
        String[] dataArray = new String[]{"Doris","Emily","Betty","Alice","Cindy"};
        List<String> expectedDistinctNameList = expectedOutData(dataArray);
        System.out.println("期望name列表：" + expectedDistinctNameList);
        List<String> actualDistinctNameList = funcObj.distinctNameFunc();
        System.out.println("实际name列表：" + actualDistinctNameList);

        Assert.assertTrue(actualDistinctNameList.equals(expectedDistinctNameList));
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按age字段去重查询结果正确")
    public void test04DistinctAgeFunc() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {18,22,39,24,25,32};
        List<Integer> expectedDistinctAgeList = expectedOutData(dataArray);
        System.out.println("期望age列表：" + expectedDistinctAgeList);
        List<Integer> actualDistinctAgeList = funcObj.distinctAgeFunc();
        System.out.println("实际age列表：" + actualDistinctAgeList);

        Assert.assertTrue(actualDistinctAgeList.equals(expectedDistinctAgeList));
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求平均结果正确")
    public void test05AvgFunc() throws SQLException, ClassNotFoundException {
        int expectedAvgAge = 25;
        int actualAvgAge = funcObj.avgAgeFunc();
        System.out.println("实际平均年龄：" + actualAvgAge);

        Assert.assertEquals(actualAvgAge, expectedAvgAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求平均结果正确")
    public void test06SumFunc() throws SQLException, ClassNotFoundException {
        int expectedSumAge = 225;
        int actualSumAge = funcObj.sumAgeFunc();
        System.out.println("实际年龄总和：" + actualSumAge);

        Assert.assertEquals(actualSumAge, expectedSumAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求最大值结果正确")
    public void test07MaxFunc() throws SQLException, ClassNotFoundException {
        int expectedMaxAge = 39;
        int actualMaxAge = funcObj.maxAgeFunc();
        System.out.println("实际最大年龄：" + actualMaxAge);

        Assert.assertEquals(actualMaxAge, expectedMaxAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求最小值结果正确")
    public void test08MinFunc() throws SQLException, ClassNotFoundException {
        int expectedMinAge = 18;
        int actualMinAge = funcObj.minAgeFunc();
        System.out.println("实际最小年龄：" + actualMinAge);

        Assert.assertEquals(actualMinAge, expectedMinAge);
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按年龄升序排序")
    public void test09OrderAscFunc() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {18,18,22,22,24,25,25,32,39};
        List<Integer> expectedOrderAscAgeList = expectedOutData(dataArray);
        System.out.println("期望age升序列表：" + expectedOrderAscAgeList);
        List<Integer> actualOrderAscAgeList = funcObj.orderAscFunc();
        System.out.println("实际age升序列表：" + actualOrderAscAgeList);

        Assert.assertTrue(actualOrderAscAgeList.equals(expectedOrderAscAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按年龄降序排序")
    public void test10OrderDescFunc() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {39,32,25,25,24,22,22,18,18};
        List<Integer> expectedOrderDescAgeList = expectedOutData(dataArray);
        System.out.println("期望age降序列表：" + expectedOrderDescAgeList);
        List<Integer> actualOrderDescAgeList = funcObj.orderDescFunc();
        System.out.println("实际age降序列表：" + actualOrderDescAgeList);

        Assert.assertTrue(actualOrderDescAgeList.equals(expectedOrderDescAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证非排序下取指定limit条数")
    public void test11LimitFunc() throws SQLException, ClassNotFoundException {
        String[] dataArray = new String[]{"Alice","Betty","Cindy","Doris","Emily"};
        List<String> expectedLimitNameList = expectedOutData(dataArray);
        System.out.println("期望数据输出：" + expectedLimitNameList);
        List<String> actualLimitList = funcObj.limitWithoutOrderAndOffsetFunc();
        System.out.println("实际数据输出：" + actualLimitList);

        Assert.assertTrue(actualLimitList.equals(expectedLimitNameList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证降序排序下取Top1")
    public void test12OrderLimitTop1Func() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {39};
        List<Integer> expectedOrderLimitAgeList = expectedOutData(dataArray);
        System.out.println("期望数据输出：" + expectedOrderLimitAgeList);
        List<Integer> actualOrderLimitList = funcObj.orderLimitWithoutOffsetFunc();
        System.out.println("实际数据输出：" + actualOrderLimitList);

        Assert.assertTrue(actualOrderLimitList.equals(expectedOrderLimitAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证降序排序下按指定偏移量取指定条数")
    public void test13OrderLimitOffsetFunc() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {25,24};
        List<Integer> expectedOrderLimitOffsetAgeList = expectedOutData(dataArray);
        System.out.println("期望数据输出：" + expectedOrderLimitOffsetAgeList);
        List<Integer> actualOrderLimitOffsetList = funcObj.orderLimitWithOffsetFunc();
        System.out.println("实际数据输出：" + actualOrderLimitOffsetList);

        Assert.assertTrue(actualOrderLimitOffsetList.equals(expectedOrderLimitOffsetAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证分组查询并排序")
    public void test14GroupOrderFunc() throws SQLException, ClassNotFoundException {
        Double[] dataArray1 = {5.2, 5.8, 8.1, 11.0, 16.9};
        List<Double> expectedGroupAmountList = expectedOutData(dataArray1);

        String[] dataArray2 = new String[]{"Doris","Emily","Cindy","Betty","Alice"};
        List<String> expectedGroupNameList = expectedOutData(dataArray2);

        System.out.println("期望分组后amount列表输出：" + expectedGroupAmountList);
        System.out.println("期望分组后name列表输出：" + expectedGroupNameList);
        List<Double> actualGroupAmountList = funcObj.groupOrderAmountFunc();
        List<String> actualGroupNameList = funcObj.groupOrderNameFunc();
        System.out.println("实际分组后amount列表输出：" + actualGroupAmountList);
        System.out.println("实际分组后namet列表输出：" + actualGroupNameList);

        Assert.assertTrue(actualGroupAmountList.equals(expectedGroupAmountList));
        Assert.assertTrue(actualGroupNameList.equals(expectedGroupNameList));
    }

    @Test(dependsOnGroups = {"funcs"}, description = "验证按指定姓名条件删除数据成功")
    public void test15DeleteWithNameCondition() throws SQLException, ClassNotFoundException {
        int expectedDeleteNameCount = 3;
        int actualDeleteNameCount = funcObj.deleteWithNameCondition();

        Assert.assertEquals(actualDeleteNameCount, expectedDeleteNameCount);
    }

    @Test (groups = {"postFuncs"},dependsOnMethods = {"test15DeleteWithNameCondition"}, description = "验证按姓名删除后的查询全表结果正确")
    public void test16QueryAllAfterDeleteWithNameCondition() throws SQLException, ClassNotFoundException {
        String[] dataArray = new String[]{"Betty","Cindy","Doris","Emily","Betty","Cindy"};
        List<String> expectedDeleteNameList = expectedOutData(dataArray);
        System.out.println("期望数据输出：" + expectedDeleteNameList);
        List<String> actualDeleteNameList = funcObj.getNameListAfterDeleteName();
        System.out.println("实际数据输出：" + actualDeleteNameList);

        Assert.assertTrue(actualDeleteNameList.equals(expectedDeleteNameList));
    }

    @Test(groups = {"postFuncs"}, dependsOnMethods = {"test16QueryAllAfterDeleteWithNameCondition"}, description = "验证cast函数")
    public void test17CastFunc() throws SQLException, ClassNotFoundException {
        int expectedCastNum = 133;
        int actualCastNum = funcObj.castFunc();

        Assert.assertEquals(actualCastNum, expectedCastNum);
    }


    /**
     * v0.2.0补充测试用例
     */
    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"}, description = "验证表为空时，取最小值，返回Null")
    public void testCase065() throws SQLException, ClassNotFoundException {
        String actualMin = funcObj.case065();
        Assert.assertNull(actualMin);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，取最大值，返回Null")
    public void testCase069() throws SQLException, ClassNotFoundException {
        String actualMax = funcObj.case069();
        Assert.assertNull(actualMax);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，取age字段加和，返回Null")
    public void testCase073() throws SQLException, ClassNotFoundException {
        String actualSum = funcObj.case073();
        Assert.assertNull(actualSum);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，取age字段求平均，返回Null")
    public void testCase259() throws SQLException, ClassNotFoundException {
        String actualAvg = funcObj.case259();
        Assert.assertNull(actualAvg);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，count统计返回0")
    public void testCase074() throws SQLException, ClassNotFoundException {
        int actualCount = funcObj.case074();
        Assert.assertEquals(actualCount, 0);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，升序返回空")
    public void testCase263() throws SQLException, ClassNotFoundException {
        Boolean actualAscOrder = funcObj.case263();
        Assert.assertFalse(actualAscOrder);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，降序返回空")
    public void testCase272() throws SQLException, ClassNotFoundException {
        Boolean actualDescOrder = funcObj.case272();
        Assert.assertFalse(actualDescOrder);
    }

    @Test(enabled = true, groups = {"emp"}, dependsOnMethods = {"test00CreateFuncTable2"},
            description = "验证表为空时，分组查询")
    public void testCase281() throws SQLException, ClassNotFoundException {
        Boolean actualGroup = funcObj.case281();
        Assert.assertFalse(actualGroup);
    }

    @Test(enabled = true, groups = {"single"}, dependsOnMethods = {"test00InsertSingleTable2"},
            description = "验证表中只有一条数据，查询最小值")
    public void testCase066() throws SQLException, ClassNotFoundException {
        int actualMinAge = funcObj.case066();
        Assert.assertEquals(actualMinAge, 18);
    }

    @Test(enabled = true, groups = {"single"}, dependsOnMethods = {"test00InsertSingleTable2"},
            description = "验证表中只有一条数据，查询最大值")
    public void testCase070() throws SQLException, ClassNotFoundException {
        int actualMaxAge = funcObj.case070();
        Assert.assertEquals(actualMaxAge, 18);
    }

    @Test(enabled = true, groups = {"single"}, dependsOnMethods = {"test00InsertSingleTable2"},
            description = "验证表中只有一条数据，字段升序")
    public void testCase262() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        expectedOrderList.add(18);
        List actualOrderList = funcObj.case262();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"single"}, dependsOnMethods = {"test00InsertSingleTable2"},
            description = "验证表中只有一条数据，字段降序")
    public void testCase271() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        expectedOrderList.add(18);
        List actualOrderList = funcObj.case271();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证对varchar类型字段查询最小值")
    public void testCase067() throws SQLException, ClassNotFoundException {
        String expectedMinName = "  aB c  dE ";
        System.out.println("Expected: " + expectedMinName);
        String actualMinName = funcObj.case067();
        System.out.println("Actual: " + actualMinName);
        Assert.assertEquals(actualMinName, expectedMinName);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证对varchar类型字段查询最大值")
    public void testCase071() throws SQLException, ClassNotFoundException {
        String expectedMaxNamge = "zhngsna";
        System.out.println("Expected: " + expectedMaxNamge);
        String actualMaxName = funcObj.case071();
        System.out.println("Actual: " + actualMaxName);
        Assert.assertEquals(actualMaxName, expectedMaxNamge);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证对double类型字段查询最小值")
    public void testCase068() throws SQLException, ClassNotFoundException {
        Double expectedMinAmount = 0.0;
        System.out.println("Expected: " + expectedMinAmount);
        Double actualMinAmount = funcObj.case068();
        System.out.println("Actual: " + actualMinAmount);
        Assert.assertEquals(actualMinAmount, expectedMinAmount);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证对double类型字段查询最大值")
    public void testCase072() throws SQLException, ClassNotFoundException {
        Double expectedMaxAmount = 2345.0;
        System.out.println("Expected: " + expectedMaxAmount);
        Double actualMaxAmount = funcObj.case072();
        System.out.println("Actual: " + actualMaxAmount);
        Assert.assertEquals(actualMaxAmount, expectedMaxAmount);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证将double类型转为整型")
    public void testCase136() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {24, 895, 123, 9, 1454, 0, 2, 12, 109, 1234, 100, 2345, 9, 32, 0};
        List expectedCastList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedCastList);
        List actualCastList = funcObj.case136();
        System.out.println("Actual: " + actualCastList);

        Assert.assertTrue(actualCastList.containsAll(expectedCastList));
        Assert.assertTrue(expectedCastList.containsAll(actualCastList));

    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证将int类型转为double类型")
    public void testCase137() throws SQLException, ClassNotFoundException {
        Double[] dataArray = {18.0, 25.0, 55.0, 57.0, 1.0, 544.0, 76.0, 18.0, 76.0, 256.0,
                61.0, 2.0, 57.0, 99.0, 18.0};
        List expectedCastList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedCastList);
        List actualCastList = funcObj.case137();

        Assert.assertTrue(actualCastList.containsAll(expectedCastList));
        Assert.assertTrue(expectedCastList.containsAll(actualCastList));

    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求和")
    public void testCase257() throws SQLException, ClassNotFoundException {
        funcObj.case257();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求和,即使都为数值字符串")
    public void testCase258() throws SQLException, ClassNotFoundException {
        funcObj.case258();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求平均")
    public void testCase260() throws SQLException, ClassNotFoundException {
        funcObj.case260();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求平均,即使都为数值字符串")
    public void testCase261() throws SQLException, ClassNotFoundException {
        funcObj.case261();
    }


    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证varchar类型字段升序")
    public void testCase265() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"11","  aB c  dE "},{"12"," abcdef"},{"15","1.5"},{"6","123"}, {"4","HAHA"},
                {"13","HAHA"},{"5","awJDs"}, {"3","l3"},{"2","lisi"},{"10","lisi"},{"9","op "},{"7","yamaha"},
                {"1","zhangsan"},{"8","zhangsan"},{"14","zhngsna"}
        };
        List<List> expectedList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedList);
        List<List> actualOrderList = funcObj.case265();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证varchar类型字段降序")
    public void testCase273() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"14","zhngsna"},{"1","zhangsan"},{"8","zhangsan"},{"7","yamaha"},{"9","op "},
                {"2","lisi"},{"10","lisi"},{"3","l3"},{"5","awJDs"},{"4","HAHA"},{"13","HAHA"},{"6","123"},
                {"15","1.5"},{"12"," abcdef"},{"11","  aB c  dE "}
        };
        List<List> expectedList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedList);
        List<List> actualOrderList = funcObj.case273();
        Assert.assertEquals(actualOrderList, expectedList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证按两个字段升序")
    public void testCase266() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"5","awJDs","1","1453.9999"},{"12"," abcdef","2","2345.0"},{"15","1.5","18","0.1235"},
                {"8","zhangsan","18","12.3"},{"1","zhangsan","18","23.5"}, {"2","lisi","25","895.0"},
                {"3","l3","55","123.123"}, {"4","HAHA","57","9.0762556"},{"13","HAHA","57","9.0762556"},
                {"11","  aB c  dE ","61","99.9999"}, {"7","yamaha","76","2.3"}, {"9","op ","76","109.325"},
                {"14","zhngsna","99","32.0"},{"10","lisi","256","1234.456"},{"6","123","544","0.0"}};
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case266();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证按两个字段降序")
    public void testCase274() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"9","op ","76","109.325","wuhan"},
                {"7","yamaha","76","2.3","beijing changyang"},{"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case274();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证按三个字段升序")
    public void testCase267() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"5","awJDs","1","1453.9999","pingYang1"},{"12"," abcdef","2","2345.0","123"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"8","zhangsan","18","12.3","shanghai"},
                {"1","zhangsan","18","23.5","Beijing"},{"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"4","HAHA","57","9.0762556","CHANGping"},
                {"13","HAHA","57","9.0762556","CHANGping"},{"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"9","op ","76","109.325","wuhan"},{"7","yamaha","76","2.3","beijing changyang"},
                {"14","zhngsna","99","32.0","chong qing "},{"10","lisi","256","1234.456","nanjing"},
                {"6","123","544","0.0","543"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case267();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证按三个字段降序")
    public void testCase275() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"}, {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case275();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证升序缺少字段，预期异常")
    public void testCase268() throws SQLException, ClassNotFoundException {
        funcObj.case268();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证降序缺少字段，预期异常")
    public void testCase276() throws SQLException, ClassNotFoundException {
        funcObj.case276();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证升序字段重复")
    public void testCase269() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {1,2,18,18,18,25,55,57,57,61,76,76,99,256,544};
        List expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List actualOrderList = funcObj.case269();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList,expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证降序字段重复")
    public void testCase277() throws SQLException, ClassNotFoundException {
        Integer[] dataArray = {544,256,99,76,76,61,57,57,55,25,18,18,18,2,1};
        List expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List actualOrderList = funcObj.case277();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList,expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证升序降序同时使用")
    public void testCase279_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"}, {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"8","zhangsan","18","12.3","shanghai"},
                {"1","zhangsan","18","23.5","Beijing"}, {"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case279_1();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证升序降序同时使用")
    public void testCase279_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},{"12"," abcdef","2","2345.0","123"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"6","123","544","0.0","543"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},{"3","l3","55","123.123","wuhan NO.1 Street"},
                {"10","lisi","256","1234.456","nanjing"},{"2","lisi","25","895.0"," beijing haidian "},
                {"9","op ","76","109.325","wuhan"},{"7","yamaha","76","2.3","beijing changyang"},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"14","zhngsna","99","32.0","chong qing "}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case279_2();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证升序降序同时使用,字段相同")
    public void testCase280_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"6", "544"},{"10", "256"},{"14", "99"},{"7", "76"},{"9", "76"},{"11", "61"},{"4", "57"},
                {"13", "57"},{"3", "55"},{"2", "25"},{"1", "18"},{"8", "18"},{"15", "18"},{"12", "2"},
                {"5", "1"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case280_1();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证升序降序同时使用,字段相同")
    public void testCase280_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"5", "1"},{"12", "2"},{"1", "18"},{"8", "18"},{"15", "18"},{"2", "25"},{"3", "55"},{"4", "57"},
                {"13", "57"},{"11", "61"},{"7", "76"},{"9", "76"},{"14", "99"},{"10", "256"},{"6", "544"}
        };
        List<List> expectedOrderList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case280_2();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "验证使用聚合字段分组，预期失败")
    public void testCase282() throws SQLException, ClassNotFoundException {
        funcObj.case282();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "使用非查询字段分组")
    public void testCase283() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"281", "2129.456"},{"2", "2345.0"},{"55", "123.123"},{"1", "1453.9999"},{"36", "35.8"},
                {"114", "18.1525112"},{"76", "2.3"},{"61", "99.9999"},
                {"99", "32.0"},{"544", "0.0"},{"18", "0.1235"},{"76", "109.325"}};

        List<List> expectedGroupList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case283();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "全表分组，预期失败")
    public void testCase284() throws SQLException, ClassNotFoundException {
        funcObj.case284();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "使用多字段分组")
    public void testCase285_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"lisi", "25", "895.0"},{"zhngsna","99","32.0"},{"awJDs","1","1453.9999"},
                {"1.5","18","0.1235"},{"lisi","256","1234.456"},
                {"l3","55","123.123"},{"zhangsan","18","35.8"},{"123","544","0.0"},
                {"op ","76","109.325"},{"yamaha","76","2.3"},{"HAHA","57","18.1525112"},
                {"  aB c  dE ","61","99.9999"},{" abcdef","2","2345.0"}};

        List<List> expectedGroupList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case285_1();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "使用多字段分组")
    public void testCase285_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan","18","23.5","Beijing"},{" abcdef","2","2345.0","123"},
                {"1.5","18","0.1235","http://WWW.baidu.com"},{"zhngsna","99","32.0","chong qing "},
                {"lisi","25","895.0"," beijing haidian "},{"123","544","0.0","543"},
                {"op ","76","109.325","wuhan"},{"awJDs","1","1453.9999","pingYang1"},
                {"yamaha","76","2.3","beijing changyang"}, {"  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"l3","55","123.123","wuhan NO.1 Street"},{"zhangsan","18","12.3","shanghai"},
                {"HAHA","57","9.0762556","CHANGping"}, {"lisi","256","1234.456","nanjing"},
        };
        List<List> expectedGroupList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case285_2();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateGroupTable1"}, description = "分组使用having过滤")
    public void testGroupWithHaving() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zala","189"},{"liuliu","179"},{"zhangsan","178"}
        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.groupWithHaving();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }


    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "limit超过数据条数")
    public void testCase286() throws SQLException, ClassNotFoundException {
        int actualRowNum = funcObj.case286();
        System.out.println("Actual: " + actualRowNum);
        Assert.assertEquals(actualRowNum, 15);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "offset小于等于数据总条数")
    public void testCase289() throws SQLException, ClassNotFoundException {
        int actualRowNum1 = funcObj.case289_1();
        int actualRowNum2 = funcObj.case289_2();
        int actualRowNum3 = funcObj.case289_3();
        System.out.println("Actual: " + actualRowNum1);
        System.out.println("Actual: " + actualRowNum2);
        System.out.println("Actual: " + actualRowNum3);
        Assert.assertEquals(actualRowNum1, 5);
        Assert.assertEquals(actualRowNum2, 1);
        Assert.assertEquals(actualRowNum3, 14);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "offset超过数据条数")
    public void testCase290() throws SQLException, ClassNotFoundException {
        Boolean actualResult = funcObj.case290();
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "limit 0条")
    public void testCase291() throws SQLException, ClassNotFoundException {
        Boolean actualResult1 = funcObj.case291_1();
        Boolean actualResult2 = funcObj.case291_2();
        System.out.println("Actual: " + actualResult1);
        System.out.println("Actual: " + actualResult2);
        Assert.assertFalse(actualResult1);
        Assert.assertFalse(actualResult2);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "limit 负数")
    public void testCase292_1() throws SQLException, ClassNotFoundException {
        funcObj.case292_1();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "limit 负数")
    public void testCase292_2() throws SQLException, ClassNotFoundException {
        funcObj.case292_2();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证limit限制小数")
    public void testCase293_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan", "18"}, {"lisi", "25"}, {"l3", "55"}
        };

        List<List> expectedLimitList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedLimitList);
        List<List> actualLimitList = funcObj.case293_1();
        System.out.println("Actual: " + actualLimitList);
        Assert.assertEquals(actualLimitList,expectedLimitList);
//        Assert.assertTrue(actualLimitList.containsAll(expectedLimitList));
//        Assert.assertTrue(expectedLimitList.containsAll(actualLimitList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证limit限制小数")
    public void testCase293_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},
                {"6","123","544","0.0","543"}
        };
        List<List> expectedLimitList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedLimitList);
        List<List> actualLimitList = funcObj.case293_2();
        System.out.println("Actual: " + actualLimitList);
        Assert.assertEquals(actualLimitList, expectedLimitList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "limit 字符串")
    public void testCase294_1() throws SQLException, ClassNotFoundException {
        funcObj.case294_1();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "limit 字符串")
    public void testCase294_2() throws SQLException, ClassNotFoundException {
        funcObj.case294_2();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "offset 为负数")
    public void testCase295_1() throws SQLException, ClassNotFoundException {
        funcObj.case295_1();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "offset 为字符串")
    public void testCase295_2() throws SQLException, ClassNotFoundException {
        funcObj.case295_2();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "offset 为小数")
    public void testCase295_3() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"HAHA","57"},
                {"awJDs","1"},
                {"123","544"}
        };
        List<List> expectedLimitList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedLimitList);
        List<List> actualLimitList = funcObj.case295_3();
        System.out.println("Actual: " + actualLimitList);
        Assert.assertEquals(actualLimitList,expectedLimitList);
//        Assert.assertTrue(actualLimitList.containsAll(expectedLimitList));
//        Assert.assertTrue(expectedLimitList.containsAll(actualLimitList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "缺失参数，预期异常")
    public void testCase296_1() throws SQLException, ClassNotFoundException {
        funcObj.case296_1();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "缺失参数，预期异常")
    public void testCase296_2() throws SQLException, ClassNotFoundException {
        funcObj.case296_2();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, expectedExceptions = SQLException.class,
            description = "缺失参数，预期异常")
    public void testCase296_3() throws SQLException, ClassNotFoundException {
        funcObj.case296_3();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in范围只有一个元素")
    public void testCase297() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"8","zhangsan","18","12.3","shanghai"}
        };

        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case297();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in范围有两个元素")
    public void testCase298() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"2","lisi","25","895.0"," beijing haidian "},
                {"8","zhangsan","18","12.3","shanghai"},
                {"10","lisi","256","1234.456","nanjing"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case298();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in范围有部分元素不在表里")
    public void testCase299_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"2","lisi","25","895.0"," beijing haidian "},
                {"8","zhangsan","18","12.3","shanghai"},
                {"10","lisi","256","1234.456","nanjing"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case299_1();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in范围有部分元素不在表里")
    public void testCase299_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan","18"},{"l3","55"},
                {"awJDs","1"},{"yamaha","76"},
                {"op ","76"},{"  aB c  dE ","61"},
                {"HAHA","57"},{"1.5","18"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case299_2();
        System.out.println("Actual: " + actualInList);
        Assert.assertTrue(actualInList.containsAll(expectedInList));
        Assert.assertTrue(expectedInList.containsAll(actualInList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in范围元素全不在表里")
    public void testCase300() throws SQLException, ClassNotFoundException {
        Boolean actualResult = funcObj.case300();
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "in查找多个字段")
    public void testCase301() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"10","lisi","256","1234.456","nanjing"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case301();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00CreateFuncTable3"}, description = "in范围有空字符串")
    public void testCase302() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"2","lisi","35","120.98",""},
                {"7","baba","99","23.51648",""},
                {"11","","0","0.01",""}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case302();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "not in范围只有一个元素")
    public void testCase308() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},
                {"6","123","544","0.0","543"},
                {"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"},
                {"10","lisi","256","1234.456","nanjing"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case308();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "not in范围有多个元素")
    public void testCase309_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},
                {"6","123","544","0.0","543"},
                {"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case309_1();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "not in范围有多个元素")
    public void testCase309_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"2","lisi","25","895.0"," beijing haidian "},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"6","123","544","0.0","543"},
                {"8","zhangsan","18","12.3","shanghai"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "}
        };

        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case309_2();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "not in范围元素不在表中")
    public void testCase310() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},
                {"6","123","544","0.0","543"},
                {"7","yamaha","76","2.3","beijing changyang"},
                {"8","zhangsan","18","12.3","shanghai"},
                {"9","op ","76","109.325","wuhan"},
                {"10","lisi","256","1234.456","nanjing"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case310();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "not in范围多个字段")
    public void testCase311() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"6","123","544","0.0","543"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case311();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateFuncTable3"}, description = "not in范围有空字符串")
    public void testCase312() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","90.33","beijing"},
                {"3","Hello","35","18.0","beijing"},
                {"4","HELLO2","15","23.0","chaoyangdis_1 NO.street"},
                {"5","lala","18","12.1234560987","beijing"},
                {"6","88","18","12.0","changping 89"},
                {"8","zala","100","54.0","wuwuxi "},
                {"9"," uzlia ","28","23.6","  maya"},
                {"10","  MaiTeng","66","70.3","ding TAO  "}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case312();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase312"}, description = "update中使用in范围")
    public void testCase306() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","laozhang"},
                {"2","laozhang"},
                {"6","laozhang"},
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case306();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase306"}, description = "update中使用not in范围")
    public void testCase313() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","laozhang","18","90.33","beijing"},
                {"2","laozhang","35","120.98",""},
                {"3","Hello","35","18.0","beijing"},{"4","HELLO2","15","23.0","chaoyangdis_1 NO.street"},
                {"5","lala","18","12.1234560987","beijing"},{"6","laozhang","18","12.0","changping 89"},
                {"7","baba","99","23.51648",""},{"8","zala","100","54.0","wuwuxi "},
                {"9"," uzlia ","28","23.6","BJ"},{"10","  MaiTeng","66","70.3","BJ"},
                {"11","","0","0.01",""}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case313();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase313"}, description = "delete语句使用，in关键字")
    public void testCase307() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"3","Hello","35","18.0","beijing"},{"4","HELLO2","15","23.0","chaoyangdis_1 NO.street"},
                {"8","zala","100","54.0","wuwuxi "}, {"9"," uzlia ","28","23.6","BJ"},
                {"10","  MaiTeng","66","70.3","BJ"}, {"11","","0","0.01",""}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case307();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase307"}, description = "delete语句使用，not in关键字")
    public void testCase314() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"3","Hello","35","18.0","beijing"},{"4","HELLO2","15","23.0","chaoyangdis_1 NO.street"},
                {"8","zala","100","54.0","wuwuxi "}, {"9"," uzlia ","28","23.6","BJ"},
                {"11","","0","0.01",""}
        };
        List<List> expectedInList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedInList);
        List<List> actualInList = funcObj.case314();
        System.out.println("Actual: " + actualInList);
        Assert.assertEquals(actualInList, expectedInList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "单个and")
    public void testCase315() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"8","zhangsan","18","12.3","shanghai"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case315();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个and")
    public void testCase316_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {{"8","zhangsan"}};
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case316_1();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个and")
    public void testCase316_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case316_2();
        System.out.println("Actual: " + actualAndList);
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个and")
    public void testCase316_3() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"8","zhangsan","18","12.3","shanghai"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case316_3();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "单个or")
    public void testCase319() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"8","zhangsan","18","12.3","shanghai"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case319();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个or")
    public void testCase320_1() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan"}, {"zhangsan"}, {" abcdef"}, {"1.5"}
        };
        List<List> expectedOrList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrList);
        List<List> actualOrList = funcObj.case320_1();
        System.out.println("Actual: " + actualOrList);
        Assert.assertEquals(actualOrList, expectedOrList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个or")
    public void testCase320_2() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan"}, {"lisi"}, {"l3"}, {"HAHA"},
                {"awJDs"}, {"123"}, {"yamaha"}, {"zhangsan"},
                {"op "}, {"lisi"}, {"  aB c  dE "}, {" abcdef"},
                {"HAHA"}, {"zhngsna"}, {"1.5"}
        };
        List<List> expectedOrList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedOrList);
        List<List> actualOrList = funcObj.case320_2();
        System.out.println("Actual: " + actualOrList);
        Assert.assertEquals(actualOrList, expectedOrList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "and和or一起使用")
    public void testCase323() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"8","zhangsan","18","12.3","shanghai"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case323();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "验证支持括号")
    public void testCase324() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"}
        };
        List<List> expectedAndList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedAndList);
        List<List> actualAndList = funcObj.case324();
        System.out.println("Actual: " + actualAndList);
        Assert.assertEquals(actualAndList, expectedAndList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "所有字段去重")
    public void testCase325() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","zhangsan","18","23.5","Beijing"},
                {"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},
                {"6","123","544","0.0","543"},
                {"7","yamaha","76","2.3","beijing changyang"},
                {"8","zhangsan","18","12.3","shanghai"},
                {"9","op ","76","109.325","wuhan"},
                {"10","lisi","256","1234.456","nanjing"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case325();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "多个字段去重")
    public void testCase326() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan","18","23.5","Beijing"},
                {"lisi","25","895.0"," beijing haidian "},
                {"l3","55","123.123","wuhan NO.1 Street"},
                {"HAHA","57","9.0762556","CHANGping"},
                {"awJDs","1","1453.9999","pingYang1"},
                {"123","544","0.0","543"},
                {"yamaha","76","2.3","beijing changyang"},
                {"zhangsan","18","12.3","shanghai"},
                {"op ","76","109.325","wuhan"},
                {"lisi","256","1234.456","nanjing"},
                {"  aB c  dE ","61","99.9999","beijing chaoyang"},
                {" abcdef","2","2345.0","123"},
                {"zhngsna","99","32.0","chong qing "},
                {"1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case326();
        System.out.println("Actual: " + actualDataList);
        Assert.assertTrue(actualDataList.containsAll(expectedDataList));
        Assert.assertTrue(expectedDataList.containsAll(actualDataList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"},
            expectedExceptions = SQLException.class, description = "验证distinct不在所有字段前")
    public void testCase327_1() throws SQLException, ClassNotFoundException {
        funcObj.case327_1();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"},
            expectedExceptions = SQLException.class, description = "验证distinct不在所有字段前")
    public void testCase327_2() throws SQLException, ClassNotFoundException {
        funcObj.case327_2();
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"},
            description = "验证count和distinct一起使用")
    public void testCase329() throws SQLException, ClassNotFoundException {
        int actualNum = funcObj.case329();
        System.out.println("Actual: " + actualNum);
        Assert.assertEquals(actualNum,12);
    }

    @Test(enabled = true, description = "验证distinct在空表中使用")
    public void testCase330() throws SQLException, ClassNotFoundException {
        Boolean actualResult = funcObj.case330();
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase314"}, description = "无重复返回全部数据")
    public void testCase331() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"Hello"},{"HELLO2"},
                {"zala"}, {" uzlia "}, {""}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case331();
        System.out.println("Actual: " + actualDataList);
        Assert.assertTrue(actualDataList.containsAll(expectedDataList));
        Assert.assertTrue(expectedDataList.containsAll(actualDataList));
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "复合查询1")
    public void testCase332() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"zhangsan","12.3"},{"1.5","0.1235"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case332();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "复合查询2")
    public void testCase333() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"544","0.0","543"},{"18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case333();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "复合查询3")
    public void testCase334() throws SQLException, ClassNotFoundException {
        String[] dataArray = {"8","8","9","2514.356","130","14","0.0"};
        List expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List actualDataList = funcObj.case334();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase334"}, description = "插入相同主键")
    public void testCase335() throws SQLException, ClassNotFoundException {
        int expectedNum = funcObj.case335();
        Assert.assertEquals(expectedNum, 0);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase335"}, expectedExceptions = SQLException.class,
            description = "插入字段和表字段不符")
    public void testCase336_1() throws SQLException, ClassNotFoundException {
        funcObj.case336_1();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase336_1"}, expectedExceptions = SQLException.class,
            description = "插入字段和表字段不符")
    public void testCase336_2() throws SQLException, ClassNotFoundException {
        funcObj.case336_2();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase336_2"}, expectedExceptions = SQLException.class,
            description = "插入字段和表字段类型不符")
    public void testCase337() throws SQLException, ClassNotFoundException {
        funcObj.case337();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase337"}, description = "修改表数据，要修改的值不存在")
    public void testCase338() throws SQLException, ClassNotFoundException {
        int expectedNum = funcObj.case338();
        Assert.assertEquals(expectedNum, 0);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase330"}, description = "表为空，删除表")
    public void testCase339() throws SQLException, ClassNotFoundException {
        int expectedNum = funcObj.case339();
        Assert.assertEquals(expectedNum, 0);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase338"}, description = "删除空结果集")
    public void testCase340() throws SQLException, ClassNotFoundException {
        int expectedNum = funcObj.case340();
        Assert.assertEquals(expectedNum, 0);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "删除不存在的表")
    public void testCase341() throws SQLException, ClassNotFoundException {
        funcObj.case341();
    }

    @Test(enabled = true, description = "创建表后删除插入，再创建同表，原数据也删除")
    public void testCase342() throws SQLException, ClassNotFoundException {
        Boolean expectedResult = funcObj.case342();
        Assert.assertFalse(expectedResult);
    }

    @Test(enabled = true, description = "创建表未指定not null")
    public void testCase343() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {{"1","hello",null,null,null}};
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List actualDataList = funcObj.case343();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, description = "创建表指定null,有默认值")
    public void testCase344() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {{"1","hello","20",null,null}};
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List actualDataList = funcObj.case344();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, description = "创建表指定not null,有默认值")
    public void testCase624() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {{"1","hello","20",null,"shanghai"}};
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List actualDataList = funcObj.case624();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, groups = {"multi"}, dependsOnMethods = {"test00InsertMultiTable2"}, description = "select语句插入数据")
    public void testCase1049() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"6","123","544","0.0","543"},
                {"7","yamaha","76","2.3","beijing changyang"},
                {"8","zhangsan","18","12.3","shanghai"},
                {"9","op ","76","109.325","wuhan"},
                {"10","lisi","256","1234.456","nanjing"},
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"12"," abcdef","2","2345.0","123"},
                {"13","HAHA","57","9.0762556","CHANGping"},
                {"14","zhngsna","99","32.0","chong qing "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case1049();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase1049"}, description = "删除后，插入相同数据")
    public void testCase346() throws SQLException, ClassNotFoundException {
        int expectedNum = funcObj.case346();
        Assert.assertEquals(expectedNum, 1);
    }

    @Test(enabled = true, description = "count字段不计入null")
    public void testCase1119() throws SQLException, ClassNotFoundException {
        String[] dataArray = new String[] {"2", "4", "4"};
        List expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List actualDataList = funcObj.case1119();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase346"}, description = "or在update中使用")
    public void testCase321() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"1","new"},{"8","new"},{"15","new"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case321();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase321"}, description = "or在delete中使用")
    public void testCase322() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},
                {"4","HAHA","57","9.0762556","CHANGping"},
                {"13","HAHA","57","9.0762556","CHANGping"}
        };
        List<List> expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case322();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, description = "非主键插入null，可成功")
    public void testCase1443_1() throws SQLException, ClassNotFoundException {
        String[] dataArray = new String[] {"1", null, null, null, null, null, null, null, null};
        List expectedDataList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.case1443_1();
        System.out.println("Actual: " + actualDataList);
        Assert.assertEquals(actualDataList, expectedDataList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase1443_1"}, expectedExceptions = SQLException.class,
            description = "主键插入null，预期异常")
    public void testCase1443_2() throws SQLException, ClassNotFoundException {
        funcObj.case1443_2();
    }

    @Test(enabled = true, description = "验证宽表创建和数据插入成功")
    public void testcase1483_widthTableCreate() throws SQLException, ClassNotFoundException {
        String width_meta_path = "src/test/resources/tabledata/meta/sqlOperation/width_table_meta.txt";
        String width_value_path = "src/test/resources/tabledata/value/sqlOperation/width_table_value.txt";
        String width_meta = FileReaderUtil.readFile(width_meta_path);
        String width_value = FileReaderUtil.readFile(width_value_path);
        funcObj.createWidthTable(width_meta);
        int effectRows = funcObj.insertWidthTable(width_value);
        Assert.assertEquals(effectRows, 4);
    }

    @Test(enabled = true, dependsOnMethods = {"testcase1483_widthTableCreate"},description = "验证宽表查询")
    public void testCase1483_widthTableQuery() throws SQLException, ClassNotFoundException {
        String[][] dataArray = {
                {"2","Wangwu","37234819900621890X","2021-12-31"},
                {"4","lisi","100122200108110375","2022-12-31"}
        };
        List<List> expectedDataList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            columnList.add(false);
            expectedDataList.add(columnList);
        }
        System.out.println("Expected: " + expectedDataList);
        List<List> actualDataList = funcObj.searchWidthTable();
        System.out.println("Actual: " + actualDataList);
        Assert.assertTrue(actualDataList.containsAll(expectedDataList));
        Assert.assertTrue(expectedDataList.containsAll(actualDataList));
    }

    @Test(enabled = true, dataProvider = "yamlSQLFuncMethod", dependsOnMethods = {"test00CreateQueryTable1"},
            description = "验证按时间日期等值查询")
    public void testQueryDateTimeEQCondition(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedQueryList = strTo2DList.construct2DList(param.get("dataStr"),";",",");
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryDateTimeEQCondition(param.get("queryColumn"),
                param.get("eqValue"),param.get("testField"));
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dataProvider = "yamlSQLFuncMethod", dependsOnMethods = {"test00CreateQueryTable1"},
            description = "验证对各类型进行非等值查询")
    public void testQueryTypeNECondition(Map<String, String> param) throws SQLException {
        int expectedRows = Integer.parseInt(param.get("rowNum"));
        System.out.println("Expected: " + expectedRows);

        int actualRows = funcObj.queryTypeNECondition(param.get("queryColumn"), param.get("neValue"));
        System.out.println("Actual: " + actualRows);
        Assert.assertEquals(actualRows, expectedRows);
    }

    @Test(enabled = true, dataProvider = "yamlSQLFuncMethod", dependsOnMethods = {"test00CreateQueryTable1"},
            description = "验证时间日期按in范围查询")
    public void testQueryDateTimeInRangeCondition(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedQueryList = strTo2DList.construct2DList(param.get("dataStr"), ";",",");
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryDateTimeInRangeCondition(param.get("queryColumn"),
                param.get("inRange"),param.get("testField"));
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dataProvider = "yamlSQLFuncMethod", dependsOnMethods = {"test00CreateQueryTable1"},
            description = "验证时间日期按not in范围查询")
    public void testQueryDateTimeNotInRangeCondition(Map<String, String> param) throws SQLException {
        int expectedRows = Integer.parseInt(param.get("rowNum"));
        System.out.println("Expected: " + expectedRows);

        int actualRows = funcObj.queryDateTimeNotInRangeCondition(param.get("queryColumn"),
                param.get("notInRange"),param.get("testField"));
        System.out.println("Actual: " + actualRows);
        Assert.assertEquals(actualRows, expectedRows);
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateQueryTable1"}, description = "查询布尔类型字段值为null的数据")
    public void testQueryBooleanNull() throws SQLException {
        String[][] dataArray = {
                {"7",null,null,null,null,"1949-01-01","00:30:08","2022-12-01 01:02:03",null},
                {"14","Sity","15","2000.0","beijing changyang","1949-10-01","12:30:00","2022-12-31 23:59:59",null}
        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryBooleanNullValue();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateQueryTable1"}, description = "字符型字段in范围查询含有null值")
    public void testQueryVarcharInRangeWithNull() throws SQLException {
        String[][] dataArray = {
                {"1","zhangsan",null,"23.5","beijing","1998-04-06", "08:10:10", "2022-04-08 18:05:07", "true"},
                {"8","wangwu","44","1000.0","beijing","2015-09-10", "03:45:10", "2001-11-11 18:05:07", "true"},
                {"12","Kelay","10","87231.0","Yang GU", "2018-05-31", "21:00:00", "2000-01-01 00:00:00", "true"}
        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryVarcharInRangeWithNull();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateQueryTable1"}, description = "整型字段in范围查询含有null值")
    public void testQueryIntInRangeWithNull() throws SQLException {
        String[][] dataArray = {
                {"9","Steven","20",null," beijing haidian ", "1995-12-15", "16:35:38", "2008-08-08 08:00:00", "true"},
                {"15","Public","18","100.0","beijing ","2007-08-15", "22:10:10", "2020-02-29 05:53:44", "true"},
                {"17","1.5","120","500.0","JinMen", "2022-03-01", "15:20:20", "1953-10-21 16:10:28", "true"}
        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryIntInRangeWithNull();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateQueryTable1"}, description = "浮点型字段in范围查询含有null值")
    public void testQueryDoubleInRangeWithNull() throws SQLException {
        String[][] dataArray = {
                {"5","awJDs","1","1453.9999","pingYang1", "2010-10-01", "19:00:00", "2010-10-01 02:02:02", "true"},
                {"6","123","60","0.0","543", "1987-07-16", "01:02:03", "1952-12-31 12:12:12", "true"},
                {"15","Public","18","100.0","beijing ","2007-08-15", "22:10:10", "2020-02-29 05:53:44", "true"},
                {"19","Adidas","1","1453.9999","pingYang1", "2010-10-01", "19:00:00", "2010-10-01 02:02:02", "false"},
                {"21","Zala",null,"2000.01","JiZhou", "2022-07-07", "00:00:00", "2022-07-07 13:30:03", "false"}
        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryDoubleInRangeWithNull();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dependsOnMethods = {"test00CreateQueryTable1"}, description = "布尔型字段in范围查询含有null值")
    public void testQueryBooleanInRangeWithNull() throws SQLException {
        String[][] dataArray = {
                {"2","lisi","false"},{"3","li3","false"},{"10","3M","false"},
                {"11",null,"false"},{"13"," Nigula","false"},{"16","Juliya","false"},
                {"18","777","false"},{"19","Adidas","false"},{"21","Zala","false"},

        };
        List<List> expectedQueryList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedQueryList);

        List<List> actualQueryList = funcObj.queryBooleanInRangeWithNull();
        System.out.println("Actual: " + actualQueryList);
        Assert.assertTrue(actualQueryList.containsAll(expectedQueryList));
        Assert.assertTrue(expectedQueryList.containsAll(actualQueryList));
    }

    @Test(enabled = true, dataProvider = "yamlSQLFuncMethod", dependsOnMethods = {"test00CreateQueryTable1"},
            description = "验证其他类型按not in范围查询，字段值为null的数据不返回")
    public void testQueryTypeNotInRangeCondition(Map<String, String> param) throws SQLException {
        int expectedRows = Integer.parseInt(param.get("rowNum"));
        System.out.println("Expected: " + expectedRows);

        int actualRows = funcObj.queryTypeNotInRangeCondition(param.get("queryColumn"), param.get("notInRange"));
        System.out.println("Actual: " + actualRows);
        Assert.assertEquals(actualRows, expectedRows);
    }

    @AfterClass(description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        String tableName = funcObj.getFuncTableName();
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();

        try {
            tearDownStatement = SQLFuncs.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

//            tearDownStatement.execute("drop table " + tableName);
//            tearDownStatement.execute("drop table emptest065");
//            tearDownStatement.execute("drop table test302");
//            tearDownStatement.execute("drop table case330");
//        //tearDownStatement.execute("drop table case341");
//            tearDownStatement.execute("drop table case342");
//            tearDownStatement.execute("drop table case343");
//            tearDownStatement.execute("drop table case344");
//            tearDownStatement.execute("drop table case624");
//            tearDownStatement.execute("drop table case1049");
//            tearDownStatement.execute("drop table case1119");
//            tearDownStatement.execute("drop table case1443");
//            tearDownStatement.execute("drop table case1483");
//            tearDownStatement.execute("drop table querytest1");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.closeResource(SQLFuncs.connection, tearDownStatement);
        }
    }
}
