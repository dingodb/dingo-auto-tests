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
import io.dingodb.dailytest.BetweenState;
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

public class TestBetweenAndState extends YamlDataHelper {
    public static BetweenState betweenObj = new BetweenState();

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        betweenObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        betweenObj.insertTableValues(tableName, insertFields, tableValues);
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
    public static void setupAll() {
        Assert.assertNotNull(BetweenState.connection);
    }

    @Test(priority = 0, enabled = true, dataProvider = "yamlBetweenMethod", description = "创建表并插入数据")
    public void test00TableCreate(Map<String, String> param) throws SQLException, InterruptedException {
        String tableMetaPath = param.get("metaPath");
        String tableValuePath = param.get("valuePath");
        initTable(param.get("tableName"), tableMetaPath);
        Thread.sleep(5000);
        insertTableValue(param.get("tableName"), param.get("insertFields"), tableValuePath);
    }

    @Test(priority = 0, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证使用between and子句按主键范围查询")
    public void test01BetweenQueryByPrimaryKeyRange() throws SQLException {
        String[][] dataArray = {{"3", "li3"},{"4", "HAHA"}, {"5", "awJDs"}, {"6", "123"}, {"7", "yamaha"}};
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByPrimaryKeyRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按整型字段值进行范围查询")
    public void test02BetweenQueryByIntRange() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi", "25"},{"3", "li3", "55"}, {"4", "HAHA", "57"}, {"6", "123", "60"},
                {"8", "wangwu", "44"},{"9", "Steven", "20"},{"10", "3M", "31"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByIntRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按浮点型字段值进行范围查询")
    public void test03BetweenQueryByDoubleRange() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi", "895.0"},{"3", "li3", "123.123"}, {"5", "awJDs", "1453.9999"},
                {"8", "wangwu", "1000.0"},{"9", "Steven", "2000.0"},{"14", "Sity", "2000.0"},
                {"15", "Public", "100.0"},{"16", "Juliya", "1999.99"},{"17", "1.5", "500.0"},
                {"19", "Adidas", "1453.9999"},{"20", "Bilibili", "100.0"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByDoubleRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按字符型字段值进行范围查询")
    public void test04BetweenQueryByStrRange1() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi"},{"3", "li3"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange1();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按字符型字段值进行范围查询")
    public void test04BetweenQueryByStrRange2() throws SQLException {
        String[][] dataArray = {
                {"4","HAHA"},{"9","Steven"},{"10", "3M"},
                {"11","GiGi"},{"12","Kelay"},{"14","Sity"},
                {"15","Public"},{"16","Juliya"},{"18", "777"},
                {"19","Adidas"},{"20","Bilibili"},{"21","Zala"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange2();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按字符型字段值进行范围查询")
    public void test04BetweenQueryByStrRange3() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "beijing"},{"8", "wangwu", "beijing"}, {"10", "3M", "Lasa"},
                {"12", "Kelay", "Yang GU"},{"15", "Public", "beijing"}, {"20", "Bilibili", "Xuchang"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange3();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按Date字段值进行范围查询")
    public void test05BetweenQueryByDateRange() throws SQLException {
        String[][] dataArray = {
                {"4", "HAHA", "2020-11-11"}, {"5", "awJDs", "2010-10-01"}, {"8", "wangwu", "2015-09-10"},
                {"12", "Kelay", "2018-05-31"}, {"13", " Nigula", "2014-10-13"},{"18", "777", "2020-11-11"},
                {"19", "Adidas", "2010-10-01"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByDateRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按Time字段值进行范围查询")
    public void test06BetweenQueryByTimeRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "08:10:10"}, {"2", "lisi", "06:15:08"}, {"3", "li3", "07:03:15"},
                {"5", "awJDs", "19:00:00"}, {"9", "Steven", "16:35:38"}, {"10", "3M", "17:30:15"},
                {"11", "GiGi", "06:00:00"},{"14", "Sity", "12:30:00"}, {"16", "Juliya", "14:09:49"},
                {"17", "1.5", "15:20:20"},{"19", "Adidas", "19:00:00"}, {"20", "Bilibili", "11:11:00"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByTimeRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between按TimeStamp字段值进行范围查询")
    public void test07BetweenQueryByTimeStampRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "2022-04-08 18:05:07"}, {"2", "lisi", "2000-02-29 00:00:00"},
                {"4", "HAHA", "2021-05-04 12:00:00"},{"5", "awJDs", "2010-10-01 02:02:02"},
                {"8", "wangwu", "2001-11-11 18:05:07"}, {"9", "Steven", "2008-08-08 08:00:00"},
                {"12", "Kelay", "2000-01-01 00:00:00"},{"15", "Public", "2020-02-29 05:53:44"},
                {"16", "Juliya", "2000-02-29 00:00:00"}, {"18", "777", "2021-05-04 12:00:00"},
                {"19", "Adidas", "2010-10-01 02:02:02"},{"21", "Zala", "2022-07-07 13:30:03"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByTimeStampRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询范围起始值大于终止值返回空集")
    public void test08BetweenStartGTEnd(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryStartGTEnd(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询范围起始值等于终止值")
    public void test08BetweenStartEQEnd(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedBetweenList = strTo2DList.construct2DList(param.get("dataStr"), ";", ",");
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryStartEQEnd(param.get("betweenState"), param.get("testField"));
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 8, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询全范围返回全部")
    public void test09BetweenQueryFullRange(Map<String, String> param) throws SQLException {
        int actualQueryRows = betweenObj.betweenQueryByFullRange(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryRows);

        Assert.assertEquals(actualQueryRows, 21);
    }

    @Test(priority = 9, enabled = false, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证查询范围时间日期值无效，返回空集")
    public void test10BetweenQueryInvalidDateTime(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryInvalidDateTime(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 10, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证查询范围未匹配到数据返回空集")
    public void test11BetweenNoValueMatch(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryNoValueMatched(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证使用not between and子句按主键范围查询")
    public void test12NotBetweenQueryByPrimaryKeyRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan"},{"2", "lisi"}, {"18", "777"},
                {"19", "Adidas"}, {"20", "Bilibili"},{"21", "Zala"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByPrimaryKeyRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证使用not between and子句按整型范围查询")
    public void test13NotBetweenQueryByIntRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan","18"},{"5", "awJDs", "1"},{"7", "yamaha", "76"},{"11", "GiGi", "98"},
                {"12", "Kelay", "10"},{"13", " Nigula", "100"},{"14", "Sity", "15"},{"15", "Public", "18"},
                {"16", "Juliya", "82"},{"17", "1.5", "120"},{"18", "777", "77"},{"19", "Adidas", "1"},
                {"20", "Bilibili", "200"},{"21", "Zala", "76"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByIntRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between按浮点型字段值进行范围查询")
    public void test14NotBetweenQueryByDoubleRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan","23.5"},{"4", "HAHA", "9.0762556"},{"6", "123", "0.0"},{"7", "yamaha", "2.3"},
                {"10", "3M", "20010.0"},{"11", "GiGi", "4201.98"},{"12", "Kelay", "87231.0"},{"13", " Nigula", "98.99"},
                {"18", "777", "77.77"},{"21", "Zala", "2000.01"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByDoubleRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }


    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证NotBetween按字符型字段值进行范围查询")
    public void test15NotBetweenQueryByStrRange1() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan"},{"4", "HAHA"},{"5", "awJDs"},{"6", "123"},{"7", "yamaha"},{"8", "wangwu"},
                {"9", "Steven"},{"10", "3M"},{"11","GiGi"},{"12","Kelay"}, {"13", " Nigula"},{"14", "Sity"},
                {"15", "Public"},{"16","Juliya"},{"17", "1.5"}, {"18", "777"},{"19", "Adidas"},
                {"20","Bilibili"},{"21", "Zala"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证NotBetween按字符型字段值进行范围查询")
    public void test15NotBetweenQueryByStrRange2() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan"},{"2", "lisi"},{"3", "li3"},{"5", "awJDs"},
                {"6", "123"},{"7", "yamaha"},{"8", "wangwu"}, {"13", " Nigula"},{"17", "1.5"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证NotBetween按字符型字段值进行范围查询")
    public void test15NotBetweenQueryByStrRange3() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi", "haidian"},{"3", "li3", "wuhan NO.1 Street"},{"4","HAHA","CHANGping"},
                {"5", "awJDs", "pingYang1"}, {"6", "123", "543"},{"7","yamaha","beijing changyang"},
                {"9", "Steven", " beijing haidian "},{"11", "GiGi", "Huhe"}, {"13", " Nigula", "Alaska"},
                {"14", "Sity", "beijing changyang"},{"16","Juliya","Huluodao"},{"17","1.5","JinMen"},
                {"18", "777", "7788"}, {"19", "Adidas", "pingYang1"},{"21", "Zala", "JiZhou"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange3();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between按Date字段值进行范围查询")
    public void test16NotBetweenQueryByDateRange() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "1998-04-06"},{"2", "lisi", "1988-02-05"},
                {"3", "li3", "2022-03-04"}, {"6", "123", "1987-07-16"},{"7", "yamaha", "1949-01-01"},
                {"9", "Steven", "1995-12-15"},{"10", "3M", "2021-03-04"}, {"11", "GiGi", "1976-07-07"},
                {"14", "Sity", "1949-10-01"},{"15", "Public", "2007-08-15"},{"16", "Juliya", "1960-11-11"},
                {"17", "1.5", "2022-03-01"},{"20", "Bilibili", "1987-12-11"},{"21", "Zala", "2022-07-07"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByDateRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between按Time字段值进行范围查询")
    public void test17NotBetweenQueryByTimeRange() throws SQLException {
        String[][] dataArray = {
                {"4", "HAHA", "05:59:59"},{"6", "123", "01:02:03"},{"7", "yamaha", "00:30:08"},
                {"8", "wangwu", "03:45:10"},{"12", "Kelay", "21:00:00"}, {"13", " Nigula", "01:00:00"},
                {"15", "Public", "22:10:10"},{"18", "777", "05:59:59"}, {"21", "Zala", "00:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByTimeRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between按TimeStamp字段值进行范围查询")
    public void test18NotBetweenQueryByTimeStampRange() throws SQLException {
        String[][] dataArray = {
                {"3", "li3", "1999-02-28 23:59:59"},{"6", "123", "1952-12-31 12:12:12"},
                {"7", "yamaha", "2022-12-01 01:02:03"}, {"10", "3M", "1999-02-28 00:59:59"},
                {"11", "GiGi", "2024-05-04 12:00:00"}, {"13", " Nigula", "1999-12-31 23:59:59"},
                {"14", "Sity", "2022-12-31 23:59:59"},{"17", "1.5", "1953-10-21 16:10:28"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByTimeStampRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 18, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询范围起始值大于终止值返回全部")
    public void test19NotBetweenStartGTEnd(Map<String, String> param) throws SQLException {
        int actualQueryRows = betweenObj.notBetweenQueryStartGTEnd(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryRows);

        Assert.assertEquals(actualQueryRows, 21);
    }

    @Test(priority = 18, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询范围起始值等于终止值返回除起止值外的全部")
    public void test19NotBetweenStartEQEnd(Map<String, String> param) throws SQLException {
        int expectedQueryRows = Integer.parseInt(param.get("rowCount"));
        List actualNotBetweenList = betweenObj.notBetweenQueryStartEQEnd(param.get("betweenState"), param.get("testField"));
        Assert.assertEquals(actualNotBetweenList.size(), expectedQueryRows);
        Assert.assertFalse(actualNotBetweenList.contains(param.get("testValue")));
    }

    @Test(priority = 19, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between输入值范围超过数据实际范围返回空")
    public void test20NotBetweenQueryFullRange(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryByFullRange(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 20, enabled = false, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询范围起止时间日期值均无效，返回空集")
    public void test21NotBetweenQueryInvalidDateTime(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryInvalidDateTime(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 21, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询起始日期无效，按另一个区间范围返回数据")
    public void test22NotBetweenQueryInvalidStartDate() throws SQLException {
        String[][] dataArray = {
                {"3", "li3", "2022-03-04"}, {"4", "HAHA", "2020-11-11"},{"8", "wangwu", "2015-09-10"},
                {"10", "3M", "2021-03-04"}, {"12", "Kelay", "2018-05-31"}, {"13", " Nigula", "2014-10-13"},
                {"17", "1.5", "2022-03-01"},{"18", "777", "2020-11-11"},{"21", "Zala", "2022-07-07"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartDate();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 22, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询终止日期无效，按另一个区间范围返回数据")
    public void test23NotBetweenQueryInvalidEndDate() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "1998-04-06"}, {"2", "lisi", "1988-02-05"},{"6", "123", "1987-07-16"},
                {"7", "yamaha", "1949-01-01"}, {"9", "Steven", "1995-12-15"}, {"11", "GiGi", "1976-07-07"},
                {"14", "Sity", "1949-10-01"},{"16", "Juliya", "1960-11-11"},{"20", "Bilibili", "1987-12-11"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndDate();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 23, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询起始时间无效，按另一个区间范围返回数据")
    public void test24NotBetweenQueryInvalidStartTime() throws SQLException {
        String[][] dataArray = {
                {"5", "awJDs", "19:00:00"},{"9", "Steven", "16:35:38"},{"10", "3M", "17:30:15"},
                {"12", "Kelay", "21:00:00"},{"15", "Public", "22:10:10"},{"16", "Juliya", "14:09:49"},
                {"17", "1.5", "15:20:20"},{"19", "Adidas", "19:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTime();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 24, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询终止时间无效，按另一个区间范围返回数据")
    public void test25NotBetweenQueryInvalidEndTime() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "08:10:10"},{"2", "lisi", "06:15:08"},{"3", "li3", "07:03:15"},
                {"4", "HAHA", "05:59:59"},{"6", "123", "01:02:03"},{"7", "yamaha", "00:30:08"},
                {"8", "wangwu", "03:45:10"},{"11", "GiGi", "06:00:00"}, {"13", " Nigula", "01:00:00"},
                {"18", "777", "05:59:59"},{"20", "Bilibili", "11:11:00"}, {"21", "Zala", "00:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTime();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 25, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询起始timestamp无效，按另一个区间范围返回数据")
    public void test26NotBetweenQueryInvalidStartTimestamp1() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "2022-04-08 18:05:07"},{"4", "HAHA", "2021-05-04 12:00:00"},
                {"7", "yamaha", "2022-12-01 01:02:03"}, {"11", "GiGi", "2024-05-04 12:00:00"},
                {"14", "Sity", "2022-12-31 23:59:59"},{"15", "Public", "2020-02-29 05:53:44"},
                {"18", "777", "2021-05-04 12:00:00"},{"21", "Zala", "2022-07-07 13:30:03"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTimestamp1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 26, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询起始timestamp无效，按另一个区间范围返回数据")
    public void test27NotBetweenQueryInvalidStartTimestamp2() throws SQLException {
        String[][] dataArray = {
                {"11", "GiGi", "2024-05-04 12:00:00"},
                {"14", "Sity", "2022-12-31 23:59:59"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTimestamp2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 27, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询终止timestamp无效，按另一个区间范围返回数据")
    public void test28NotBetweenQueryInvalidEndTimestamp1() throws SQLException {
        String[][] dataArray = {
                {"3", "li3", "1999-02-28 23:59:59"},{"6", "123", "1952-12-31 12:12:12"},
                {"10", "3M", "1999-02-28 00:59:59"},{"12", "Kelay", "2000-01-01 00:00:00"},
                {"13", " Nigula", "1999-12-31 23:59:59"}, {"17", "1.5", "1953-10-21 16:10:28"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTimestamp1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 28, enabled = false, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询终止timestamp无效，按另一个区间范围返回数据")
    public void test29NotBetweenQueryInvalidEndTimestamp2() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi", "2000-02-29 00:00:00"},{"3", "li3", "1999-02-28 23:59:59"},
                {"5", "awJDs", "2010-10-01 02:02:02"},{"6", "123", "1952-12-31 12:12:12"},
                {"8", "wangwu", "2001-11-11 18:05:07"}, {"9", "Steven", "2008-08-08 08:00:00"},
                {"10", "3M", "1999-02-28 00:59:59"},{"12", "Kelay", "2000-01-01 00:00:00"},
                {"13", " Nigula", "1999-12-31 23:59:59"},{"16", "Juliya", "2000-02-29 00:00:00"},
                {"17", "1.5", "1953-10-21 16:10:28"}, {"19", "Adidas", "2010-10-01 02:02:02"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTimestamp2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 29, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询范围未匹配到数据返回空集")
    public void test30NotBetweenNoValueMatch(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryNoValueMatched(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 30, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询日期范围支持的其他日期格式")
    public void test31BetweenSupportOtherDateFormat(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedBetweenList = strTo2DList.construct2DList(param.get("dataStr"), ";", ",");
        System.out.println("Expected: " + expectedBetweenList);

        List<List> actualBetweenList = betweenObj.betweenQuerySupportOtherDateFormat(param.get("queryColumn"),
                param.get("startDate"), param.get("endDate"),param.get("testField"));
        System.out.println("Actual: " + actualBetweenList);
        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 31, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询日期范围支持的其他日期格式")
    public void test32NotBetweenSupportOtherDateFormat(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedNotBetweenList = strTo2DList.construct2DList(param.get("dataStr"), ";",",");
        System.out.println("Expected: " + expectedNotBetweenList);

        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySupportOtherDateFormat(param.get("queryColumn"),
                param.get("startDate"), param.get("endDate"),param.get("testField"));
        System.out.println("Actual: " + actualNotBetweenList);
        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 32, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            expectedExceptions = SQLException.class, description = "验证参数不正确，预期失败")
    public void test33IncorrectParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.queryIncorrectParam(param.get("betweenState"));
    }

    @Test(priority = 33, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证任意参数为Null返回空集")
    public void test34BetweenNullParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 34, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "not between查询当起始值其中一个为null，返回另一个非null值的范围数据")
    public void test35NotBetweenSingleNullParam1() throws SQLException {
        String[][] dataArray = {
                {"2", "lisi", "25"},{"4", null, "57"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 35, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "not between查询当起始值其中一个为null，返回另一个非null值的范围数据")
    public void test36NotBetweenSingleNullParam1() throws SQLException {
        String[][] dataArray = {{"1", "zhangsan"}};
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 36, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "not between查询当起始值其中一个为null，返回另一个非null值的范围数据")
    public void test37NotBetweenSingleNullParam3() throws SQLException {
        String[][] dataArray = {{"1", "zhangsan", "18"},{"2", "lisi", "25"}};
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue3();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 37, enabled = true, dataProvider = "yamlBetweenMethod",dependsOnMethods = {"test00TableCreate"},
            description = "验证not between起止参数均为Null返回空集")
    public void test38NotBetweenBothNullParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryBothNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 38, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00TableCreate"},
            description = "验证整个字段所有值为Null，返回空集")
    public void test39ColumnNull(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.queryColumnNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 39, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询，字段值为null记录跳过")
    public void test40BetweenColumnPartNull() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "18"},{"4", "HAHA", "25"}, {"6", "123", "20"}
        };
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.betweenQueryColumnPartNull();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 40, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询，字段值为null记录跳过")
    public void test41NotBetweenColumnPartNull() throws SQLException {
        String[][] dataArray = {{"3", "li3", "33"},{"5", "awJDs", "44"}};
        List<List> expectedNotBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryColumnPartNull();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 41, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证多个between and查询")
    public void test42MultiBetweenQuery() throws SQLException {
        String[][] dataArray = {
                {"8", "wangwu", "44", "1000.0", "beijing", "2015-09-10", "03:45:10", "2001-11-11 18:05:07", "true"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.multiBetweenQuery();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 42, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证多个not between and查询")
    public void test43MultiNotBetweenQuery() throws SQLException {
        String[][] dataArray = {
                {"16", "Juliya", "82", "1999.99", "Huluodao", "1960-11-11", "14:09:49", "2000-02-29 00:00:00", "false"},
                {"19", "Adidas", "1", "1453.9999", "pingYang1", "2010-10-01", "19:00:00", "2010-10-01 02:02:02", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.multiNotBetweenQuery();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 43, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between和not between一起使用进行查询")
    public void test44BetweenAndNotBetweenQueryTogether1() throws SQLException {
        String[][] dataArray = {
                {"6", "123", "60", "0.0", "543", "1987-07-16", "01:02:03", "1952-12-31 12:12:12", "true"},
                {"10", "3M", "31", "20010.0", "Lasa", "2021-03-04", "17:30:15", "1999-02-28 00:59:59", "false"},
                {"3", "li3", "55", "123.123", "wuhan NO.1 Street", "2022-03-04", "07:03:15", "1999-02-28 23:59:59", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndNotBetweenQueryTogether1();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 44, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between和not between一起使用进行查询")
    public void test45BetweenAndNotBetweenQueryTogether2() throws SQLException {
        String[][] dataArray = {
                {"8", "wangwu", "44", "1000.0", "beijing", "2015-09-10", "03:45:10", "2001-11-11 18:05:07", "true"},
                {"14", "Sity", "15", "2000.0", "beijing changyang", "1949-10-01", "12:30:00", "2022-12-31 23:59:59", "true"},
                {"15", "Public", "18", "100.0", "beijing", "2007-08-15", "22:10:10", "2020-02-29 05:53:44", "true"},
                {"7", "yamaha", "76", "2.3", "beijing changyang", "1949-01-01", "00:30:08", "2022-12-01 01:02:03", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndNotBetweenQueryTogether2();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 45, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询使用聚合函数")
    public void test46BetweenQueryWithAggrFunc() throws SQLException {
        String[] dataArray = new String[] {"68", "117773.7299", "1949-10-01"};
        List expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List actualBetweenList = betweenObj.betweenQueryWithAggrFunc();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 46, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询使用聚合函数")
    public void test47NotBetweenQueryWithAggrFunc() throws SQLException {
        String[] dataArray = new String[] {"2201.22", "33018.24", "17:30:15", "1952-12-31 12:12:12"};
        List expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List actualBetweenList = betweenObj.notBetweenQueryWithAggrFunc();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 47, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between查询使用分组")
    public void test48BetweenQueryWithGroup() throws SQLException {
        String[][] dataArray = {
                {"beijing", "1123.5"},{"Xuchang", "100.0"},{"pingYang1","2907.9998"},
                {"beijing changyang", "2002.3"},{"Yang GU", "87231.0"},{"haidian","895.0"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryWithGroup();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 48, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between查询使用分组")
    public void test49NotBetweenQueryWithGroup() throws SQLException {
        String[][] dataArray = {
                {"pingYang1", "1"},{"beijing", "31"},
                {"wuhan NO.1 Street", "55"}, {"beijing changyang", "76"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenQueryWithGroup();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList,expectedBetweenList);
    }

    @Test(priority = 49, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证between语句在update语句中使用,更新字符串")
    public void test50BetweenInCharUpdateState() throws SQLException {
        String[][] dataArray = {
                {"1", "Java", "18", "BJ"},{"2", "Java", "25", "BJ"},
                {"3", "Java", "55", "BJ"},{"4", "Java", "57", "BJ"},
                {"5", "Java", "1", "BJ"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateCharState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 50, enabled = true, dependsOnMethods = {"test50BetweenInCharUpdateState"},
            description = "验证between语句在update语句中使用,更新数值型")
    public void test51BetweenInNumUpdateState() throws SQLException {
        String[][] dataArray = {
                {"6", "123", "35", "1234.5678"},{"7", "yamaha", "35", "1234.5678"},
                {"8", "wangwu", "35", "1234.5678"},{"9", "Steven", "35", "1234.5678"},
                {"10", "3M", "35", "1234.5678"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateNumState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 51, enabled = true, dependsOnMethods = {"test51BetweenInNumUpdateState"},
            description = "验证between语句在update语句中使用,更新日期时间型")
    public void test52BetweenInDateTimeUpdateState() throws SQLException {
        String[][] dataArray = {
                {"11", "GiGi", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"12", "Kelay", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"13", " Nigula", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"14", "Sity", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"15", "Public", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateDateTimeState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 52, enabled = true, dependsOnMethods = {"test52BetweenInDateTimeUpdateState"},
            description = "验证between语句在update语句中使用,更新布尔型")
    public void test53BetweenInBooleanUpdateState() throws SQLException {
        String[][] dataArray = {
                {"16", "Juliya", "true"},{"17", "1.5", "true"},{"18", "777", "true"},
                {"19", "Adidas", "true"},{"20", "Bilibili", "true"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateBooleanState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 53, enabled = true, dependsOnMethods = {"test53BetweenInBooleanUpdateState"},
            description = "验证not between语句在update语句中使用,更新字符串")
    public void test54NotBetweenInCharUpdateState() throws SQLException {
        String[][] dataArray = {
                {"6", "C++", "35", "SH"},{"7", "C++", "35", "SH"},
                {"8", "C++", "35", "SH"},{"9", "C++", "35", "SH"},
                {"10", "C++", "35", "SH"},{"11", "C++", "98", "SH"},
                {"12", "C++", "10", "SH"},{"13", "C++", "100", "SH"},
                {"14", "C++", "15", "SH"},{"15", "C++", "18", "SH"},
                {"16", "C++", "82", "SH"},{"17", "C++", "120", "SH"},
                {"18", "C++", "77", "SH"},{"19", "C++", "1", "SH"},
                {"20", "C++", "200", "SH"},{"21", "C++", "76", "SH"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateCharState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 54, enabled = true, dependsOnMethods = {"test54NotBetweenInCharUpdateState"},
            description = "验证not between语句在update语句中使用,更新数值型")
    public void test55NotBetweenInNumUpdateState() throws SQLException {
        String[][] dataArray = {
                {"1", "Java", "66", "7890.0123"},{"2", "Java", "66", "7890.0123"},
                {"3", "Java", "66", "7890.0123"},{"4", "Java", "66", "7890.0123"},
                {"5", "Java", "66", "7890.0123"},{"11", "C++", "66", "7890.0123"},
                {"12", "C++", "66", "7890.0123"},{"13", "C++", "66", "7890.0123"},
                {"14", "C++", "66", "7890.0123"},{"15", "C++", "66", "7890.0123"},
                {"16", "C++", "66", "7890.0123"},{"17", "C++", "66", "7890.0123"},
                {"18", "C++", "66", "7890.0123"},{"19", "C++", "66", "7890.0123"},
                {"20", "C++", "66", "7890.0123"},{"21", "C++", "66", "7890.0123"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateNumState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 55, enabled = true, dependsOnMethods = {"test55NotBetweenInNumUpdateState"},
            description = "验证not between语句在update语句中使用,更新日期时间型")
    public void test56NotBetweenInDateTimeUpdateState() throws SQLException {
        String[][] dataArray = {
                {"1", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"2", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"3", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"4", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"5", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"6", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"7", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"8", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"9", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"10", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"16", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"17", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"18", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"19", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"20", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"21", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateDateTimeState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 56, enabled = true, dependsOnMethods = {"test56NotBetweenInDateTimeUpdateState"},
            description = "验证not between语句在update语句中使用,更新布尔型")
    public void test57NotBetweenInBooleanUpdateState() throws SQLException {
        String[][] dataArray = {
                {"1", "Java", "false"}, {"2", "Java", "false"}, {"3", "Java", "false"},
                {"4", "Java", "false"}, {"5", "Java", "false"}, {"6", "C++", "false"},
                {"7", "C++", "false"}, {"8", "C++", "false"}, {"9", "C++", "false"},
                {"10", "C++", "false"}, {"11", "C++", "false"}, {"12", "C++", "false"},
                {"13", "C++", "false"}, {"14", "C++", "false"}, {"15", "C++", "false"},
                {"21", "C++", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateBooleanState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 57, enabled = true, dependsOnMethods = {"test57NotBetweenInBooleanUpdateState"},
            description = "验证between语句更新范围集为空")
    public void test58BetweenUpdateEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenUpdateEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 58, enabled = true, dependsOnMethods = {"test58BetweenUpdateEmpSet"},
            description = "验证not between语句更新范围集为空")
    public void test59BetweenUpdateEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenUpdateEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 59, enabled = true, dependsOnMethods = {"test59BetweenUpdateEmpSet"},
            description = "验证between在删除语句使用")
    public void test60BetweenInDeleteNotEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenDeleteNotEmpSet();
        System.out.println("ActualEffected: " + actualEffectRows);
        Assert.assertEquals(actualEffectRows, 5);

        String[][] dataArray = {
                {"6", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"7", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"8", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"9", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"10", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"11", "C++", "66", "7890.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"12", "C++", "66", "7890.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"13", "C++", "66", "7890.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"14", "C++", "66", "7890.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"15", "C++", "66", "7890.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"16", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"17", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"18", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"19", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"20", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"21", "C++", "66", "7890.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualAfterBetweenDeleteList = betweenObj.betweenQueryAfterDelete();
        System.out.println("Actual: " + actualAfterBetweenDeleteList);

        Assert.assertTrue(actualAfterBetweenDeleteList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualAfterBetweenDeleteList));
    }

    @Test(priority = 60, enabled = true, dependsOnMethods = {"test60BetweenInDeleteNotEmpSet"},
            description = "验证not between在删除语句使用")
    public void test61NotBetweenInDeleteNotEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenDeleteNotEmpSet();
        Assert.assertEquals(actualEffectRows, 11);

        String[][] dataArray = {
                {"6", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"7", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"8", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"9", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"10", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualAfterNotBetweenDeleteList = betweenObj.notBetweenQueryAfterDelete();
        System.out.println("Actual: " + actualAfterNotBetweenDeleteList);

        Assert.assertTrue(actualAfterNotBetweenDeleteList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualAfterNotBetweenDeleteList));
    }

    @Test(priority = 61, enabled = true, dependsOnMethods = {"test61NotBetweenInDeleteNotEmpSet"},
            description = "验证between语句删除范围集为空")
    public void test62BetweenDeleteEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenDeleteEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 62, enabled = true, dependsOnMethods = {"test62BetweenDeleteEmpSet"},
            description = "验证not between语句删除范围集为空")
    public void test63NotBetweenDeleteEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenDeleteEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 63, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between在非等值连接中使用")
    public void test64BetweenInInnerJoin() throws SQLException {
        String[][] dataArray = {
                {"100", "10000.0", "B"},{"101", "7386.0", "A"}, {"103", "18350.0", "C"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInInnerJoin();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 64, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证not between在非等值连接中使用")
    public void test65NotBetweenInInnerJoin() throws SQLException {
        String[][] dataArray = {
                {"10000.0", "A"},{"10000.0", "C"}, {"7386.0", "B"},{"7386.0", "C"},
                {"20001.0", "A"},{"20001.0", "B"},{"20001.0", "C"},{"18350.0", "A"},
                {"18350.0", "B"},{"4500.0", "A"},{"4500.0", "B"},{"4500.0", "C"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInInnerJoin();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 65, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证3主键beteen and范围查询")
    public void test66BetweenIn3PrimaryKeyTable() throws SQLException {
        String[][] dataArray = {
                {"13834781245","zhangsan","18","23.5","2022-04-08 18:05:07"},
                {"13840980021","lisi","25","895.0","2000-02-29 00:00:00"},
                {"13901278905","HAHA","57","9.0762556","2021-05-04 12:00:00"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndIn3PrimaryKey();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 66, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证3主键not beteen and范围查询")
    public void test67NotBetweenIn3PrimaryKeyTable() throws SQLException {
        String[][] dataArray = {
                {"13587642399","Steven","20","2000.0"," beijing haidian ","1995-12-15","16:35:38","2008-08-08 08:00:00","true"},
                {"13790912302","awJDs","1","1453.9999","pingYang1","2010-10-01","19:00:00","2010-10-01 02:02:02","false"},
                {"15900902305","Gogo","86","23.45","Shanghai","2022-03-13","12:00:00","2022-12-01 10:10:10","false"},
                {"15804857822","123","544","0.0","543","1987-07-16","01:02:03","1952-12-31 12:12:12","false"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenAndIn3PrimaryKey();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 67, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证2主键beteen and范围查询")
    public void test68BetweenIn2PrimaryKeyTable() throws SQLException {
        String[][] dataArray = {
                {"300", "Cindy", "39", "4.6"},
                {"400", "Doris", "25", "5.2"},
                {"500", "Emily", "24", "5.8"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndIn2PrimaryKey();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 68, enabled = true, dependsOnMethods = {"test00TableCreate"},
            description = "验证2主键not beteen and范围查询")
    public void test69NotBetweenIn3PrimaryKeyTable() throws SQLException {
        String[][] dataArray = {
                {"100", "Alice", "18", "3.5"},
                {"200", "Betty", "22", "4.1"},
                {"600", "Alice", "32", "6.1"},
                {"700", "Betty", "18", "6.9"},
                {"800", "Alice", "22", "7.3"},
                {"900", "Cindy", "25", "3.5"}
        };
        List<List> expectedBetweenList = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenAndIn2PrimaryKey();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 69, enabled = true, dataProvider = "yamlBetweenMethod",dependsOnMethods = {"test00TableCreate"},
            description = "验证通过字符型主键进行范围查询")
    public void test70QueryUsingVarcharPrimaryKey(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = betweenObj.betweenAndUsingVarcharPrimaryKey(
                param.get("tableName"), param.get("queryKey"), param.get("startChar"), param.get("endChar"));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
//        List<String> tableList = JDBCUtils.getTableList();
        List<String> tableList = Arrays.asList(
                "betweenTest", "betweenTest2", "betweenTest3", "betweenTest4", "betweenTest5",
                "betweenTest8", "betweenTest9", "betweenTest10", "between_employees", "between_job_grades");
        try {
            tearDownStatement = BetweenState.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
//            tearDownStatement.execute("drop table betweenTest");
//            tearDownStatement.execute("drop table betweenTest2");
//            tearDownStatement.execute("drop table betweenTest3");
//            tearDownStatement.execute("drop table betweenTest4");
//            tearDownStatement.execute("drop table betweenTest5");
//            tearDownStatement.execute("drop table betweenTest8");
//            tearDownStatement.execute("drop table betweenTest9");
//            tearDownStatement.execute("drop table betweenTest10");
//            tearDownStatement.execute("drop table between_employees");
//            tearDownStatement.execute("drop table between_job_grades");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.closeResource(BetweenState.connection, tearDownStatement);
        }
    }
}
