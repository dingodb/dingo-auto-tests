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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Listeners(EmailableReporterListener.class)
public class TestSQLFuncs {
//    private static final String defaultConnectIP = "172.20.3.26";
//    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
//    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";

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
//        SQLFuncs funcsTableMultiInsert = new SQLFuncs();
        int actualMultiInsertCount = funcObj.insertMultiValues();
        Assert.assertEquals(actualMultiInsertCount, expectedMultiInsertCount);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证表格数据统计")
    public void test02CountFunc() throws SQLException, ClassNotFoundException {
        int expectedCountRows = 9;
//        SQLFuncs countFunc = new SQLFuncs();
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
//        SQLFuncs distinctName = new SQLFuncs();
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
//        SQLFuncs distinctAge = new SQLFuncs();
        List<Integer> actualDistinctAgeList = funcObj.distinctAgeFunc();
        System.out.println("实际age列表：" + actualDistinctAgeList);

        Assert.assertTrue(actualDistinctAgeList.equals(expectedDistinctAgeList));
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求平均结果正确")
    public void test05AvgFunc() throws SQLException, ClassNotFoundException {
        int expectedAvgAge = 25;
//        SQLFuncs avgFunc = new SQLFuncs();
        int actualAvgAge = funcObj.avgAgeFunc();
        System.out.println("实际平均年龄：" + actualAvgAge);

        Assert.assertEquals(actualAvgAge, expectedAvgAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求平均结果正确")
    public void test06SumFunc() throws SQLException, ClassNotFoundException {
        int expectedSumAge = 225;
//        SQLFuncs sumFunc = new SQLFuncs();
        int actualSumAge = funcObj.sumAgeFunc();
        System.out.println("实际年龄总和：" + actualSumAge);

        Assert.assertEquals(actualSumAge, expectedSumAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求最大值结果正确")
    public void test07MaxFunc() throws SQLException, ClassNotFoundException {
        int expectedMaxAge = 39;
//        SQLFuncs maxFunc = new SQLFuncs();
        int actualMaxAge = funcObj.maxAgeFunc();
        System.out.println("实际最大年龄：" + actualMaxAge);

        Assert.assertEquals(actualMaxAge, expectedMaxAge);
    }

    @Test(dependsOnMethods = {"test01MultiInsert"}, groups = {"funcs"}, description = "验证对年龄求最小值结果正确")
    public void test08MinFunc() throws SQLException, ClassNotFoundException {
        int expectedMinAge = 18;
//        SQLFuncs minFunc = new SQLFuncs();
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
//        SQLFuncs orderAscAge = new SQLFuncs();
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
//        SQLFuncs orderDescAge = new SQLFuncs();
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
//        SQLFuncs limitFunc = new SQLFuncs();
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
//        SQLFuncs orderLimitFunc = new SQLFuncs();
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
//        SQLFuncs orderLimitOffsetFunc = new SQLFuncs();
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
//        SQLFuncs groupOrderFunc = new SQLFuncs();
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
//        SQLFuncs deleteName = new SQLFuncs();
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
//        SQLFuncs queryAfterDelete = new SQLFuncs();
        List<String> actualDeleteNameList = funcObj.getNameListAfterDeleteName();
        System.out.println("实际数据输出：" + actualDeleteNameList);

        Assert.assertTrue(actualDeleteNameList.equals(expectedDeleteNameList));
    }

    @Test(groups = {"postFuncs"}, dependsOnMethods = {"test16QueryAllAfterDeleteWithNameCondition"}, description = "验证cast函数")
    public void test17CastFunc() throws SQLException, ClassNotFoundException {
        int expectedCastNum = 133;
//        SQLFuncs castFunc = new SQLFuncs();
        int actualCastNum = funcObj.castFunc();

        Assert.assertEquals(actualCastNum, expectedCastNum);
    }

    @AfterClass(description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = funcObj.getFuncTableName();
        Statement tearDownStatement = connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.close();
        connection.close();
    }

}
