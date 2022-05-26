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

import io.dingodb.dailytest.SQLFuncs;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Listeners(EmailableReporterListener.class)
public class TestSQLFuncs {
    private static Connection connection;
    public static SQLFuncs funcObj = new SQLFuncs();

    @BeforeClass()
    public static void ConnectDBAndCreateFuncTable() throws ClassNotFoundException, SQLException {
        connection = SQLFuncs.connectDB();
        funcObj.createFuncTable();
    }

    @Test(groups = {"preFuncs"},description = "验证批量插入数据到创建的数据表中")
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
        List<String> expectedDistinctNameList = new ArrayList<String>();
        String[] distinctNameArray = new String[]{"Doris","Emily","Betty","Alice","Cindy"};
        for (int i = 0; i < distinctNameArray.length; i++) {
            expectedDistinctNameList.add(distinctNameArray[i]);
        }
        System.out.println("期望name列表：" + expectedDistinctNameList);
        List<String> actualDistinctNameList = funcObj.distinctNameFunc();
        System.out.println("实际name列表：" + actualDistinctNameList);

        Assert.assertTrue(actualDistinctNameList.equals(expectedDistinctNameList));
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按age字段去重查询结果正确")
    public void test04DistinctAgeFunc() throws SQLException, ClassNotFoundException {
        List<Integer> expectedDistinctAgeList = new ArrayList<Integer>();
        int[] distinctAgeArray = new int[]{18,22,39,24,25,32};
        for (int i = 0; i < distinctAgeArray.length; i++) {
            expectedDistinctAgeList.add(distinctAgeArray[i]);
        }
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
        List<Integer> expectedOrderAscAgeList = new ArrayList<Integer>();
        int[] orderAscAgeArray = new int[]{18,18,22,22,24,25,25,32,39};
        for (int i = 0; i < orderAscAgeArray.length; i++) {
            expectedOrderAscAgeList.add(orderAscAgeArray[i]);
        }
        System.out.println("期望age升序列表：" + expectedOrderAscAgeList);
        List<Integer> actualOrderAscAgeList = funcObj.orderAscFunc();
        System.out.println("实际age升序列表：" + actualOrderAscAgeList);

        Assert.assertTrue(actualOrderAscAgeList.equals(expectedOrderAscAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证按年龄降序排序")
    public void test10OrderDescFunc() throws SQLException, ClassNotFoundException {
        List<Integer> expectedOrderDescAgeList = new ArrayList<Integer>();
        int[] orderDescAgeArray = new int[]{39,32,25,25,24,22,22,18,18};
        for (int i = 0; i < orderDescAgeArray.length; i++) {
            expectedOrderDescAgeList.add(orderDescAgeArray[i]);
        }
        System.out.println("期望age降序列表：" + expectedOrderDescAgeList);
        List<Integer> actualOrderDescAgeList = funcObj.orderDescFunc();
        System.out.println("实际age降序列表：" + actualOrderDescAgeList);

        Assert.assertTrue(actualOrderDescAgeList.equals(expectedOrderDescAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证非排序下取指定limit条数")
    public void test11LimitFunc() throws SQLException, ClassNotFoundException {
        List<String> expectedLimitNameList = new ArrayList<String>();
        String[] limitNameArray = new String[]{"Alice","Betty","Cindy","Doris","Emily"};
        for (int i = 0; i < limitNameArray.length; i++) {
            expectedLimitNameList.add(limitNameArray[i]);
        }
        System.out.println("期望数据输出：" + expectedLimitNameList);
        List<String> actualLimitList = funcObj.limitWithoutOrderAndOffsetFunc();
        System.out.println("实际数据输出：" + actualLimitList);

        Assert.assertTrue(actualLimitList.equals(expectedLimitNameList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证降序排序下取Top1")
    public void test12OrderLimitTop1Func() throws SQLException, ClassNotFoundException {
        List<Integer> expectedOrderLimitAgeList = new ArrayList<Integer>();
        int[] orderLimitAgeArray = new int[]{39};
        for (int i = 0; i < orderLimitAgeArray.length; i++) {
            expectedOrderLimitAgeList.add(orderLimitAgeArray[i]);
        }
        System.out.println("期望数据输出：" + expectedOrderLimitAgeList);
        List<Integer> actualOrderLimitList = funcObj.orderLimitWithoutOffsetFunc();
        System.out.println("实际数据输出：" + actualOrderLimitList);

        Assert.assertTrue(actualOrderLimitList.equals(expectedOrderLimitAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证降序排序下按指定偏移量取指定条数")
    public void test13OrderLimitOffsetFunc() throws SQLException, ClassNotFoundException {
        List<Integer> expectedOrderLimitOffsetAgeList = new ArrayList<Integer>();
        int[] orderLimitOffsetAgeArray = new int[]{25,24};
        for (int i = 0; i < orderLimitOffsetAgeArray.length; i++) {
            expectedOrderLimitOffsetAgeList.add(orderLimitOffsetAgeArray[i]);
        }
        System.out.println("期望数据输出：" + expectedOrderLimitOffsetAgeList);
        List<Integer> actualOrderLimitOffsetList = funcObj.orderLimitWithOffsetFunc();
        System.out.println("实际数据输出：" + actualOrderLimitOffsetList);

        Assert.assertTrue(actualOrderLimitOffsetList.equals(expectedOrderLimitOffsetAgeList));
    }

    @Test (dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证分组查询并排序")
    public void test14GroupOrderFunc() throws SQLException, ClassNotFoundException {
        List<Double> expectedGroupAmountList = new ArrayList<Double>();
        double[] groupAmountArray = new double[]{5.2,5.8,8.1,11.0,16.9};
        for (int i = 0; i < groupAmountArray.length; i++) {
            expectedGroupAmountList.add(groupAmountArray[i]);
        }
        List<String> expectedGroupNameList = new ArrayList<String>();
        String[] groupNameArray = new String[]{"Doris","Emily","Cindy","Betty","Alice"};
        for (int i = 0; i < groupNameArray.length; i++) {
            expectedGroupNameList.add(groupNameArray[i]);
        }
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
        List<String> expectedDeleteNameList = new ArrayList<String>();
        String[] deleteNameArray = new String[]{"Betty","Cindy","Doris","Emily","Betty","Cindy"};
        for (int i = 0; i < deleteNameArray.length; i++) {
            expectedDeleteNameList.add(deleteNameArray[i]);
        }
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
    @Test(enabled = true, description = "验证表为空时，取最小值，返回Null")
    public void testCase065() throws SQLException, ClassNotFoundException {
        String actualMin = funcObj.case065();
        Assert.assertNull(actualMin);
    }

    @Test(enabled = true, description = "验证表为空时，取最大值，返回Null")
    public void testCase069() throws SQLException, ClassNotFoundException {
        String actualMax = funcObj.case069();
        Assert.assertNull(actualMax);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase065","testCase069"},
            description = "验证表为空时，取age字段加和，返回Null")
    public void testCase073() throws SQLException, ClassNotFoundException {
        String actualSum = funcObj.case073();
        Assert.assertNull(actualSum);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase073"}, description = "验证表为空时，取age字段求平均，返回Null")
    public void testCase259() throws SQLException, ClassNotFoundException {
        String actualAvg = funcObj.case259();
        Assert.assertNull(actualAvg);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase259"}, description = "验证表为空时，count统计返回0")
    public void testCase074() throws SQLException, ClassNotFoundException {
        int actualCount = funcObj.case074();
        Assert.assertEquals(actualCount, 0);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase074"}, description = "验证表为空时，升序返回空")
    public void testCase263() throws SQLException, ClassNotFoundException {
        Boolean actualAscOrder = funcObj.case263();
        Assert.assertFalse(actualAscOrder);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase263"}, description = "验证表为空时，降序返回空")
    public void testCase272() throws SQLException, ClassNotFoundException {
        Boolean actualDescOrder = funcObj.case272();
        Assert.assertFalse(actualDescOrder);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase272"}, description = "验证表为空时，分组查询")
    public void testCase281() throws SQLException, ClassNotFoundException {
        Boolean actualGroup = funcObj.case281();
        Assert.assertFalse(actualGroup);
    }



    @Test(enabled = true, dependsOnMethods = {"testCase281"}, description = "验证表中只有一条数据，查询最小值")
    public void testCase066() throws SQLException, ClassNotFoundException {
        int actualMinAge = funcObj.case066();
        Assert.assertEquals(actualMinAge, 18);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase066"}, description = "验证表中只有一条数据，查询最大值")
    public void testCase070() throws SQLException, ClassNotFoundException {
        int actualMaxAge = funcObj.case070();
        Assert.assertEquals(actualMaxAge, 18);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase070"}, description = "验证表中只有一条数据，字段升序")
    public void testCase262() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        expectedOrderList.add(18);
        List actualOrderList = funcObj.case262();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase262"}, description = "验证表中只有一条数据，字段降序")
    public void testCase271() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        expectedOrderList.add(18);
        List actualOrderList = funcObj.case271();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase271"}, description = "验证对varchar类型字段查询最小值")
    public void testCase067() throws SQLException, ClassNotFoundException {
        String expectedMinName = "  aB c  dE ";
        System.out.println("Expected: " + expectedMinName);
        String actualMinName = funcObj.case067();
        System.out.println("Actual: " + actualMinName);
        Assert.assertEquals(actualMinName, expectedMinName);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证对varchar类型字段查询最大值")
    public void testCase071() throws SQLException, ClassNotFoundException {
        String expectedMaxNamge = "zhngsna";
        System.out.println("Expected: " + expectedMaxNamge);
        String actualMaxName = funcObj.case071();
        System.out.println("Actual: " + actualMaxName);
        Assert.assertEquals(actualMaxName, expectedMaxNamge);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证对double类型字段查询最小值")
    public void testCase068() throws SQLException, ClassNotFoundException {
        Double expectedMinAmount = 0.0;
        System.out.println("Expected: " + expectedMinAmount);
        Double actualMinAmount = funcObj.case068();
        System.out.println("Actual: " + actualMinAmount);
        Assert.assertEquals(actualMinAmount, expectedMinAmount);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证对double类型字段查询最大值")
    public void testCase072() throws SQLException, ClassNotFoundException {
        Double expectedMaxAmount = 2345.0;
        System.out.println("Expected: " + expectedMaxAmount);
        Double actualMaxAmount = funcObj.case072();
        System.out.println("Actual: " + actualMaxAmount);
        Assert.assertEquals(actualMaxAmount, expectedMaxAmount);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证将double类型转为整型")
    public void testCase136() throws SQLException, ClassNotFoundException {
        List expectedCastList = new ArrayList();
        Integer[] castArray = new Integer[]{23, 895, 123, 9, 1453, 0, 2, 12, 109, 1234, 99, 2345, 9, 32, 0};
        for (int i=0; i < castArray.length; i++){
            expectedCastList.add(castArray[i]);
        }
        System.out.println("Expected: " + expectedCastList);
        List actualCastList = funcObj.case136();

        Assert.assertTrue(actualCastList.containsAll(expectedCastList));
        Assert.assertTrue(expectedCastList.containsAll(actualCastList));

    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证将int类型转为double类型")
    public void testCase137() throws SQLException, ClassNotFoundException {
        List expectedCastList = new ArrayList();
        Double[] castArray = new Double[]{18.0, 25.0, 55.0, 57.0, 1.0, 544.0, 76.0, 18.0, 76.0, 256.0,
                61.0, 2.0, 57.0, 99.0, 18.0};
        for (int i=0; i < castArray.length; i++){
            expectedCastList.add(castArray[i]);
        }
        System.out.println("Expected: " + expectedCastList);
        List actualCastList = funcObj.case137();

        Assert.assertTrue(actualCastList.containsAll(expectedCastList));
        Assert.assertTrue(expectedCastList.containsAll(actualCastList));

    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求和")
    public void testCase257() throws SQLException, ClassNotFoundException {
        funcObj.case257();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求和,即使都为数值字符串")
    public void testCase258() throws SQLException, ClassNotFoundException {
        funcObj.case258();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求平均")
    public void testCase260() throws SQLException, ClassNotFoundException {
        funcObj.case260();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证varchar类型字段不支持求平均,即使都为数值字符串")
    public void testCase261() throws SQLException, ClassNotFoundException {
        funcObj.case261();
    }


    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证varchar类型字段升序")
    public void testCase265() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {{"11","  aB c  dE "},{"12"," abcdef"},{"15","1.5"},{"6","123"}, {"4","HAHA"},
                {"13","HAHA"},{"5","awJDs"}, {"3","l3"},{"2","lisi"},{"10","lisi"},{"9","op "},{"7","yamaha"},
                {"1","zhangsan"},{"8","zhangsan"},{"14","zhngsna"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);
        List<List> actualOrderList = funcObj.case265();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证varchar类型字段降序")
    public void testCase273() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {{"14","zhngsna"},{"1","zhangsan"},{"8","zhangsan"},{"7","yamaha"},{"9","op "},
                {"2","lisi"},{"10","lisi"},{"3","l3"},{"5","awJDs"},{"4","HAHA"},{"13","HAHA"},{"6","123"},
                {"15","1.5"},{"12"," abcdef"},{"11","  aB c  dE "}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);
        List<List> actualOrderList = funcObj.case273();
        Assert.assertEquals(actualOrderList, expectedList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证按两个字段升序")
    public void testCase266() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"5","awJDs","1","1453.9999"},{"12"," abcdef","2","2345.0"},{"15","1.5","18","0.1235"},
                {"8","zhangsan","18","12.3"},{"1","zhangsan","18","23.5"}, {"2","lisi","25","895.0"},
                {"3","l3","55","123.123"}, {"4","HAHA","57","9.0762556"},{"13","HAHA","57","9.0762556"},
                {"11","  aB c  dE ","61","99.9999"}, {"7","yamaha","76","2.3"}, {"9","op ","76","109.325"},
                {"14","zhngsna","99","32.0"},{"10","lisi","256","1234.456"},{"6","123","544","0.0"}};
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case266();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证按两个字段降序")
    public void testCase274() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"9","op ","76","109.325","wuhan"},
                {"7","yamaha","76","2.3","beijing changyang"},{"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case274();
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证按三个字段升序")
    public void testCase267() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"5","awJDs","1","1453.9999","pingYang1"},{"12"," abcdef","2","2345.0","123"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"8","zhangsan","18","12.3","shanghai"},
                {"1","zhangsan","18","23.5","Beijing"},{"2","lisi","25","895.0"," beijing haidian "},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"4","HAHA","57","9.0762556","CHANGping"},
                {"13","HAHA","57","9.0762556","CHANGping"},{"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"9","op ","76","109.325","wuhan"},{"7","yamaha","76","2.3","beijing changyang"},
                {"14","zhngsna","99","32.0","chong qing "},{"10","lisi","256","1234.456","nanjing"},
                {"6","123","544","0.0","543"}
        };
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case267();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证按三个字段降序")
    public void testCase275() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"}, {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case275();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证升序缺少字段，预期异常")
    public void testCase268() throws SQLException, ClassNotFoundException {
        funcObj.case268();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证降序缺少字段，预期异常")
    public void testCase276() throws SQLException, ClassNotFoundException {
        funcObj.case276();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证升序字段重复")
    public void testCase269() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        Integer[] orderArray = new Integer[]{1,2,18,18,18,25,55,57,57,61,76,76,99,256,544};
        for (int i=0; i < orderArray.length; i++){
            expectedOrderList.add(orderArray[i]);
        }
        System.out.println("Expected: " + expectedOrderList);
        List actualOrderList = funcObj.case269();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList,expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证降序字段重复")
    public void testCase277() throws SQLException, ClassNotFoundException {
        List expectedOrderList = new ArrayList();
        Integer[] orderArray = new Integer[]{544,256,99,76,76,61,57,57,55,25,18,18,18,2,1};
        for (int i=0; i < orderArray.length; i++){
            expectedOrderList.add(orderArray[i]);
        }
        System.out.println("Expected: " + expectedOrderList);
        List actualOrderList = funcObj.case277();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList,expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证升序降序同时使用")
    public void testCase279_1() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"6","123","544","0.0","543"},{"10","lisi","256","1234.456","nanjing"},
                {"14","zhngsna","99","32.0","chong qing "},{"7","yamaha","76","2.3","beijing changyang"},
                {"9","op ","76","109.325","wuhan"}, {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"3","l3","55","123.123","wuhan NO.1 Street"},{"2","lisi","25","895.0"," beijing haidian "},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"8","zhangsan","18","12.3","shanghai"},
                {"1","zhangsan","18","23.5","Beijing"}, {"12"," abcdef","2","2345.0","123"},
                {"5","awJDs","1","1453.9999","pingYang1"}
        };
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case279_1();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证升序降序同时使用")
    public void testCase279_2() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"11","  aB c  dE ","61","99.9999","beijing chaoyang"},{"12"," abcdef","2","2345.0","123"},
                {"15","1.5","18","0.1235","http://WWW.baidu.com"},{"6","123","544","0.0","543"},
                {"4","HAHA","57","9.0762556","CHANGping"},{"13","HAHA","57","9.0762556","CHANGping"},
                {"5","awJDs","1","1453.9999","pingYang1"},{"3","l3","55","123.123","wuhan NO.1 Street"},
                {"10","lisi","256","1234.456","nanjing"},{"2","lisi","25","895.0"," beijing haidian "},
                {"9","op ","76","109.325","wuhan"},{"7","yamaha","76","2.3","beijing changyang"},
                {"1","zhangsan","18","23.5","Beijing"},{"8","zhangsan","18","12.3","shanghai"},
                {"14","zhngsna","99","32.0","chong qing "}
        };
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case279_2();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证升序降序同时使用,字段相同")
    public void testCase280_1() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"6", "544"},{"10", "256"},{"14", "99"},{"7", "76"},{"9", "76"},{"11", "61"},{"4", "57"},
                {"13", "57"},{"3", "55"},{"2", "25"},{"1", "18"},{"8", "18"},{"15", "18"},{"12", "2"},
                {"5", "1"}};
        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case280_1();
        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "验证升序降序同时使用,字段相同")
    public void testCase280_2() throws SQLException, ClassNotFoundException {
        String[][] orderArray = {
                {"5", "1"},{"12", "2"},{"1", "18"},{"8", "18"},{"15", "18"},{"2", "25"},{"3", "55"},{"4", "57"},
                {"13", "57"},{"11", "61"},{"7", "76"},{"9", "76"},{"14", "99"},{"10", "256"},{"6", "544"}};

        List<List> expectedOrderList = new ArrayList<List>();
        for(int i=0; i<orderArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<orderArray[i].length; j++) {
                columnList.add(orderArray[i][j]);
            }
            expectedOrderList.add(columnList);
        }
        System.out.println("Expected: " + expectedOrderList);
        List<List> actualOrderList = funcObj.case280_2();

        System.out.println("Actual: " + actualOrderList);
        Assert.assertEquals(actualOrderList, expectedOrderList);
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "验证使用聚合字段分组，预期失败")
    public void testCase282() throws SQLException, ClassNotFoundException {
        funcObj.case282();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "使用非查询字段分组")
    public void testCase283() throws SQLException, ClassNotFoundException {
        String[][] groupArray = {
                {"281", "2129.456"},{"2", "2345.0"},{"55", "123.123"},{"1", "1453.9999"},{"36", "35.8"},
                {"114", "18.1525112"},{"76", "2.3"},{"61", "99.9999"},
                {"99", "32.0"},{"544", "0.0"},{"18", "0.1235"},{"76", "109.325"}};

        List<List> expectedGroupList = new ArrayList<List>();
        for(int i=0; i<groupArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<groupArray[i].length; j++) {
                columnList.add(groupArray[i][j]);
            }
            expectedGroupList.add(columnList);
        }
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case283();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, expectedExceptions = SQLException.class,
            description = "全表分组，预期失败")
    public void testCase284() throws SQLException, ClassNotFoundException {
        funcObj.case284();
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "使用多字段分组")
    public void testCase285_1() throws SQLException, ClassNotFoundException {
        String[][] groupArray = {
                {"lisi", "25", "895.0"},{"zhngsna","99","32.0"},{"awJDs","1","1453.9999"},
                {"1.5","18","0.1235"},{"lisi","256","1234.456"},
                {"l3","55","123.123"},{"zhangsan","18","35.8"},{"123","544","0.0"},
                {"op ","76","109.325"},{"yamaha","76","2.3"},{"HAHA","57","18.1525112"},
                {"  aB c  dE ","61","99.9999"},{" abcdef","2","2345.0"}};

        List<List> expectedGroupList = new ArrayList<List>();
        for(int i=0; i<groupArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<groupArray[i].length; j++) {
                columnList.add(groupArray[i][j]);
            }
            expectedGroupList.add(columnList);
        }
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case285_1();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }

    @Test(enabled = true, dependsOnMethods = {"testCase067"}, description = "使用多字段分组")
    public void testCase285_2() throws SQLException, ClassNotFoundException {
        String[][] groupArray = {
                {"zhangsan","18","23.5","Beijing"},{" abcdef","2","2345.0","123"},
                {"1.5","18","0.1235","http://WWW.baidu.com"},{"zhngsna","99","32.0","chong qing "},
                {"lisi","25","895.0"," beijing haidian "},{"123","544","0.0","543"},
                {"op ","76","109.325","wuhan"},{"awJDs","1","1453.9999","pingYang1"},
                {"yamaha","76","2.3","beijing changyang"}, {"  aB c  dE ","61","99.9999","beijing chaoyang"},
                {"l3","55","123.123","wuhan NO.1 Street"},{"zhangsan","18","12.3","shanghai"},
                {"HAHA","57","9.0762556","CHANGping"}, {"lisi","256","1234.456","nanjing"},
        };

        List<List> expectedGroupList = new ArrayList<List>();
        for(int i=0; i<groupArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<groupArray[i].length; j++) {
                columnList.add(groupArray[i][j]);
            }
            expectedGroupList.add(columnList);
        }
        System.out.println("Expected: " + expectedGroupList);
        List<List> actualGroupList = funcObj.case285_2();
        System.out.println("Actual: " + actualGroupList);
        Assert.assertTrue(actualGroupList.containsAll(expectedGroupList));
        Assert.assertTrue(expectedGroupList.containsAll(actualGroupList));
    }




    @AfterClass(description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = funcObj.getFuncTableName();
        Statement tearDownStatement = connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.execute("delete from emptest065");
        tearDownStatement.execute("drop table emptest065");
        tearDownStatement.close();
        connection.close();
    }

}
