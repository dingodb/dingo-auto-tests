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
import io.dingodb.dailytest.BooleanField;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.YamlDataHelper;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestBooleanField extends YamlDataHelper {
    public static BooleanField booleanObj = new BooleanField();
    public static String tableName1 = "booleanFieldTest";

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        booleanObj.tableCreate(tableName, tableMeta);
    }

    public int insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        int insertRows = booleanObj.insertTableValues(tableName, insertFields, tableValues);
        return insertRows;
    }

    public static List expectedOutData(Object[] dataArray) {
        List expectedList = new ArrayList();
        for (int i=0; i < dataArray.length; i++){
            expectedList.add(dataArray[i]);
        }
        return expectedList;
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

    @BeforeClass(alwaysRun = true, description = "测试数据库连接")
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        Assert.assertNotNull(BooleanField.connection);
    }

    @Test(priority = 0, description = "创建布尔型测试的表")
    public void test00CreateBooleanTable() throws SQLException, ClassNotFoundException {
        String table_meta1_path = "src/test/resources/tabledata/meta/booleantype/boolean_meta1.txt";
        initTable(tableName1, table_meta1_path);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateBooleanTable"},
            description = "验证带有布尔类型字段的表的创建是否成功")
    public void test01BooleanFieldTableCreate() throws SQLException, ClassNotFoundException, IOException {
        List<String> actualTableList = JDBCUtils.getTableList();
        Assert.assertTrue(actualTableList.contains(tableName1.toUpperCase()));
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01BooleanFieldTableCreate"},
            description = "验证true型数据插入和查询成功")
    public void test02trueValuesInsertAndQuery() throws SQLException {
        String table_value1_path = "src/test/resources/tabledata/value/booleantype/boolean_value1.txt";
        int actualInsertRows = insertTableValue(tableName1, "", table_value1_path);
        Assert.assertEquals(actualInsertRows, 4);
        Boolean[] dataArray = new Boolean[] {true,true,true,true};
        List<Boolean> expectedTrueList = expectedOutData(dataArray);

        System.out.println("Expected List: " + expectedTrueList);
        List<Boolean> actualTrueList = booleanObj.getBoolValues(tableName1,"*", " where id < 5");
        System.out.println("Actual List: " + actualTrueList);

        Assert.assertEquals(actualTrueList, expectedTrueList);
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test02trueValuesInsertAndQuery"},
            description = "验证false型数据插入和查询成功")
    public void test03falseValuesInsertAndQuery() throws SQLException {
        String table_value2_path = "src/test/resources/tabledata/value/booleantype/boolean_value2.txt";
        int actualInsertRows = insertTableValue(tableName1,"", table_value2_path);
        Assert.assertEquals(actualInsertRows, 4);
        Boolean[] dataArray = new Boolean[] {false,false,false,false};
        List<Boolean> expectedFalseList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedFalseList);

        List<Boolean> actualFalseList = booleanObj.getBoolValues(tableName1,"*", " where id >4 and id < 9");
        System.out.println("Actual List: " + actualFalseList);

        Assert.assertEquals(actualFalseList, expectedFalseList);
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test03falseValuesInsertAndQuery"},
            description = "验证布尔型数据插入和查询成功")
    public void test04trueAndfalseValuesInsertAndQuery() throws SQLException {
        String table_value3_path = "src/test/resources/tabledata/value/booleantype/boolean_value3.txt";
        int actualInsertRows = insertTableValue(tableName1,"", table_value3_path);
        Assert.assertEquals(actualInsertRows, 7);
        Boolean[] dataArray = new Boolean[] {true,false,true,false,false,true,true};
        List<Boolean> expectedtrueAndfalseList = expectedOutData(dataArray);

        System.out.println("Expected List: " + expectedtrueAndfalseList);
        List<Boolean> actualtrueAndfalseList = booleanObj.getBoolValues(tableName1,"*"," where id > 8 and id < 16");
        System.out.println("Actual List: " + actualtrueAndfalseList);

        Assert.assertEquals(actualtrueAndfalseList, expectedtrueAndfalseList);
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按true值查询")
    public void test05TrueValueQuery() throws SQLException {
        String[] dataArray = new String[] {"zhangsan","lisi","lisi3","HAHA","oppo"," ab c d ","YH","yamaha"};
        List<String> expectedTrueValueConditionList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedTrueValueConditionList);
        List<String> actualtrueValueConditionList = booleanObj.queryValue(tableName1,"*"," where is_delete = true","name");
        System.out.println("Actual List: " + actualtrueValueConditionList);

        Assert.assertEquals(actualtrueValueConditionList, expectedTrueValueConditionList);
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按is true查询")
    public void test05QueryIsTrue() throws SQLException {
        String[] dataArray = new String[] {"zhangsan","lisi","lisi3","HAHA","oppo"," ab c d ","YH","yamaha"};
        List<String> expectedIsTrueConditionList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedIsTrueConditionList);
        List<String> actualIsTrueConditionList = booleanObj.queryValue(tableName1,"*"," where is_delete is true","name");
        System.out.println("Actual List: " + actualIsTrueConditionList);

        Assert.assertEquals(actualIsTrueConditionList, expectedIsTrueConditionList);
    }


    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按false值查询")
    public void test06FalseValueQuery() throws SQLException {
        String[] dataArray = new String[] {"5", "6", "7", "8", "10", "12", "13"};
        List expectedFalseValueConditionList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedFalseValueConditionList);
        List<Integer> actualfalseValueConditionList = booleanObj.queryValue(tableName1,"*"," where is_delete = false","id");
        System.out.println("Actual List: " + actualfalseValueConditionList);

        Assert.assertEquals(actualfalseValueConditionList, expectedFalseValueConditionList);
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按Is false查询")
    public void test06QueryIsFalse() throws SQLException {
        String[] dataArray = new String[] {"5", "6", "7", "8", "10", "12", "13"};
        List<Integer> expectedIsFalseConditionList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedIsFalseConditionList);
        List<Integer> actualIsFalseConditionList = booleanObj.queryValue(tableName1,"*"," where is_delete is false","id");
        System.out.println("Actual List: " + actualIsFalseConditionList);

        Assert.assertEquals(actualIsFalseConditionList, expectedIsFalseConditionList);
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按字段为真查询")
    public void test07FieldAsConditionQuery() throws SQLException {
        Boolean[] dataArray = new Boolean[] {true,true,true,true,true,true,true,true};
        List<Boolean> expectedFieldList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedFieldList);
        List<Boolean> actualFieldList = booleanObj.getBoolValues(tableName1,"*"," where is_delete");
        System.out.println("Actual List: " + actualFieldList);

        Assert.assertEquals(actualFieldList, expectedFieldList);
    }

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按字段为假查询")
    public void test08NotFieldAsConditionQuery() throws SQLException {
        Boolean[] dataArray = new Boolean[] {false,false,false,false,false,false,false};
        List<Boolean> expectedNotFieldList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedNotFieldList);
        List<Boolean> actualNotFieldList = booleanObj.getBoolValues(tableName1,"*"," where not is_delete");
        System.out.println("Actual List: " + actualNotFieldList);

        Assert.assertEquals(actualNotFieldList, expectedNotFieldList);
    }

    @Test(priority = 8, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证where 1 = 1 为true查询")
    public void test09TrueCondition() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan"},{"2", "lisi"}, {"3", "lisi3"}, {"4", "HAHA"},
                {"5", "wJDs"},{"6", "123"},{"7", "yamaha"},{"8", "zhangsan"},
                {"9", "oppo"},{"10", "lisi"},{"11", " ab c d "},{"12", " abcdef"},
                {"13", "HX K"},{"14", "YH"},{"15", "yamaha"}
        };
        List<List> expectedList = expectedOutData(dataArray);
        System.out.println("Expected List: " + expectedList);
        List<List> actualList = booleanObj.queryByOutFields(tableName1,"*","where 1 = 1", "id,name");
        System.out.println("Actual List: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 9, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证where 1 <> 1 为false查询")
    public void test10FalseCondition() throws SQLException {
        List<List> actualList = booleanObj.queryByOutFields(tableName1,"*","where 1 <> 1", "id,name");
        System.out.println("Actual List: " + actualList);

        Assert.assertEquals(actualList.size(), 0);
    }


    @Test(priority = 10, enabled = true, expectedExceptions = SQLException.class, dataProvider = "yamlBooleanMethod",
            dependsOnMethods = {"test10FalseCondition"}, description = "预期插入失败")
    public void test11InsertStrValue(Map<String, String> param) throws SQLException {
        try(Statement statement = BooleanField.connection.createStatement()) {
            String insertSql = "insert into " + tableName1 + " values (" + param.get("ID") +
                    ",'vivo',20,456.7,'shanghai','" + param.get("booleanValue") + "')";
            statement.execute(insertSql);
        }
    }

    @Test(priority = 11, enabled = true, expectedExceptions = SQLException.class, dataProvider = "yamlBooleanMethod",
            dependsOnMethods = {"test11InsertStrValue"}, description = "预期插入失败")
    public void test12InsertWrongValue(Map<String, String> param) throws SQLException {
        try(Statement statement = BooleanField.connection.createStatement()) {
            String insertSql = "insert into " + tableName1 + " values (" + param.get("ID") +
                    ",'vivo',20,456.7,'shanghai'," + param.get("booleanValue") + ")";
            statement.execute(insertSql);
        }
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证插入0，转换为False")
    public void test13ZeroValueQuery() throws SQLException {
        String table_value4_path = "src/test/resources/tabledata/value/booleantype/boolean_value4.txt";
        int actualInsertRows = insertTableValue(tableName1,"", table_value4_path);
        Assert.assertEquals(actualInsertRows, 1);
        Boolean actualZeroValue = booleanObj.getZeroValues(tableName1);
        System.out.println("Actual: " + actualZeroValue);

        Assert.assertFalse(actualZeroValue);
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            dataProvider = "yamlBooleanMethod", description = "验证插入正整数，转换为True")
    public void test14IntegerValueQuery(Map<String, String> param) throws SQLException {
        int actualInsertRows = booleanObj.insertPosIntegerValues(tableName1, param.get("ID"), param.get("booleanValue"));
        Assert.assertEquals(actualInsertRows, 1);
        Boolean actualIntegerValue = booleanObj.getIntegerValues(tableName1, param.get("ID"));
        System.out.println("Actual: " + actualIntegerValue);

        Assert.assertTrue(actualIntegerValue);
    }

    @Test(priority = 14, enabled = true, expectedExceptions = SQLException.class,
            description = "验证字段类型为bool，创建失败")
    public void test15CreateTableUsingBoolFailed() throws SQLException, ClassNotFoundException {
        String table_meta2_path = "src/test/resources/tabledata/meta/booleantype/boolean_meta2.txt";
        initTable("booltest", table_meta2_path);
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test14IntegerValueQuery"}, description = "验证布尔类型字段插入null值")
    public void test16InsertNull() throws SQLException {
        String actualQuery = booleanObj.insertNull(tableName1);
        System.out.println("Actual: " + actualQuery);
        Assert.assertNull(actualQuery);
    }


    @AfterClass (alwaysRun = true, description = "执行测试后删除数据，删除表")
    public void teardownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList("booleanFieldTest");
        try {
            tearDownStatement = BooleanField.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.closeResource(BooleanField.connection, tearDownStatement);
        }
    }
}
