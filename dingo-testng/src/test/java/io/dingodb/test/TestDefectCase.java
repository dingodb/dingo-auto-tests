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
import io.dingodb.dailytest.DefectCase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.StrTo2DList;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestDefectCase extends YamlDataHelper {
    public static DefectCase defectObj = new DefectCase();

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        defectObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        defectObj.insertTableValues(tableName, insertFields, tableValues);
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

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(DefectCase.connection);
    }

    @Test(description = "创建测试表")
    public void test00CreateDefectTable1() throws SQLException {
        defectObj.createTable0033();
    }

    @Test(priority = 0, enabled = true, dataProvider = "yamlDefectCaseMethod", description = "创建表并插入数据")
    public void test00TableCreate(Map<String, String> param) throws SQLException, InterruptedException {
        String tableMetaPath = param.get("metaPath");
        String tableValuePath = param.get("valuePath");
        initTable(param.get("tableName"), tableMetaPath);
        Thread.sleep(5000);
        insertTableValue(param.get("tableName"), param.get("insertFields"), tableValuePath);
    }

    @Test(priority = 0, enabled = true, dependsOnMethods = {"test00CreateDefectTable1"},
            expectedExceptions = SQLException.class, description = "插入值不含不允许为null的字段，预期异常")
    public void testDefect0033_1() throws SQLException {
        defectObj.defect0033_1();
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateDefectTable1"},
            expectedExceptions = SQLException.class, description = "不允许为null的字段,插入值为null，预期异常")
    public void testDefect0033_2() throws SQLException {
        defectObj.defect0033_2();

    }

    @Test(priority = 2, enabled = true, expectedExceptions = SQLException.class,
            dependsOnMethods = {"test00CreateDefectTable1"},
            description = "不插入主键，预期异常")
    public void testDefect0033_3() throws SQLException {
        defectObj.defect0033_3();

    }

    @Test(priority = 3, enabled = true, description = "验证算术运算")
    public void testDefect0046() throws SQLException {
        Integer[] dataArray = new Integer[] {2, -3087, 156, 205, 204};
        List<Integer> expectedList = expectedOutData(dataArray);

        System.out.println("Expected: " + expectedList);
        List<Integer> actualList = defectObj.defect0046();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 4, enabled = true, expectedExceptions = SQLException.class, description = "验证字段重复，创建表失败")
    public void testDefect0136() throws SQLException {
        defectObj.defect0136();
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"testDefect0033_1"},description = "验证有日期时间字段的插入")
    public void testDefect0154() throws SQLException {
        String[] dataArray = new String[] {"1", "154", "zhangsan", "18", null, null, null, null, null, null};
        List expectedList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedList);
        List actualList = defectObj.defect0154();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"testDefect0154"}, description = "验证对时间日期类型字段求最大最小值")
    public void testDefect0178_MaxMin() throws SQLException {
        String[] dataArray = new String[] {"2022-03-13", "19:00:00", "2022-12-01 10:10:10", "1949-01-01", "00:30:08", "1952-12-31 12:12:12"};
        List expectedList = expectedOutData(dataArray);

        System.out.println("Expected: " + expectedList);
        List actualList = defectObj.defect0178_max_min();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 7, enabled = true, dependsOnMethods = {"testDefect0178_MaxMin"}, description = "验证按日期字段排序")
    public void testDefect0178_OrderByDate() throws SQLException {
        String[][] dataArray = {
                {"7","1949-01-01"},{"6","1987-07-16"},{"2","1988-02-05"},{"1","1998-04-06"},
                {"5","2010-10-01"},{"4","2020-11-11"},{"3","2022-03-04"},{"8","2022-03-13"},{"154",null}
        };
        List<List> expectedList = expectedOutData(dataArray);

        System.out.println("Expected: " + expectedList);
        List actualList = defectObj.defect0178_orderByDate();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 8, enabled = true, dependsOnMethods = {"testDefect0178_MaxMin"}, description = "验证按时间字段排序")
    public void testDefect0178_OrderByTime() throws SQLException {
        String[][] dataArray = {
                {"154",null},{"5","19:00:00"},{"8","12:00:00"},{"1","08:10:10"},
                {"3","07:03:15"},{"2","06:15:08"},{"4","05:59:59"},{"6","01:02:03"},{"7","00:30:08"}
        };
        List<List> expectedList = expectedOutData(dataArray);

        System.out.println("Expected: " + expectedList);
        List actualList = defectObj.defect0178_orderByTime();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 9, enabled = true, dependsOnMethods = {"testDefect0178_MaxMin"}, description = "验证按timestamp字段排序")
    public void testDefect0178_OrderByTimeStamp() throws SQLException {
        String[][] dataArray = {
                {"8","2022-12-01 10:10:10"},{"7","2022-12-01 01:02:03"},{"1","2022-04-08 18:05:07"},
                {"4","2021-05-04 12:00:00"}, {"5","2010-10-01 02:02:02"},{"2","2000-02-29 00:00:00"},
                {"3","1999-02-28 23:59:59"},{"6","1952-12-31 12:12:12"}
        };
        List<List> expectedList = expectedOutData(dataArray);

        System.out.println("Expected: " + expectedList);
        List actualList = defectObj.defect0178_orderByTimeStamp();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 10, enabled = true, dependsOnMethods = {"testDefect0178_OrderByTimeStamp"},
            expectedExceptions = SQLException.class, description = "验证更改字段值为不支持的数值，预期异常")
    public void testDefect0186_1() throws SQLException {
        defectObj.defect0186_1();
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"testDefect0178_OrderByTimeStamp"},
            expectedExceptions = SQLException.class, description = "验证更改字段值为不支持的数值，预期异常")
    public void testDefect0186_2() throws SQLException {
        defectObj.defect0186_2();
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"testDefect0178_MaxMin"}, description = "验证时间日期类型null值插入多条")
    public void testDefect0198() throws SQLException {
        int actualRows = defectObj.defect0198();
        System.out.println("Actual: " + actualRows);
        Assert.assertEquals(actualRows, 3);
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlDefectCaseMethod", description = "验证通过in范围过滤对表进行写操作，范围元素超过19个")
    public void testDefect0563(Map<String, String> param) throws SQLException {
        int expectedRows = Integer.parseInt(param.get("effectRows"));
        System.out.println("Expected effectedRows: " + expectedRows);
        int actualRows = defectObj.writeOpRows(param.get("tableName"), param.get("execSql"));
        Assert.assertEquals(actualRows, expectedRows);

        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DListIncludeBlank(param.get("outData"),";",",");
        System.out.println("Expected Query: " + expectedList);
        List<List> actualList = defectObj.queryByOutFields(param.get("tableName"), param.get("queryFields"), param.get("queryState"), param.get("outFields"));
        System.out.println("Actual Query: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList("defect0033", "de50563");
        try {
            tearDownStatement = DefectCase.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
//            tearDownStatement.execute("drop table defect0033");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.closeResource(DefectCase.connection, tearDownStatement);
        }
    }

}
