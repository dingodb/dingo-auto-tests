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

import io.dingodb.dailytest.BooleanField;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.YamlDataHelper;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestBooleanField extends YamlDataHelper {
    public static BooleanField booleanObj = new BooleanField();

    public static List<String> getTableList() throws SQLException, ClassNotFoundException {
        List<String> tableList = new ArrayList<String>();
        DatabaseMetaData dmd = BooleanField.connection.getMetaData();
        ResultSet resultSetSchema = dmd.getSchemas();
        List<String> schemaList = new ArrayList<>();
        while (resultSetSchema.next()) {
            schemaList.add(resultSetSchema.getString(1));
        }
        System.out.println(schemaList.get(0));
        ResultSet rst = dmd.getTables(null, schemaList.get(0), "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toUpperCase());
        }
        return tableList;
    }

    @BeforeClass(alwaysRun = true, description = "测试数据库连接")
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        Assert.assertNotNull(BooleanField.connection);
    }

    @Test(description = "创建布尔型测试的表")
    public void test00CreateBooleanTable() throws SQLException, ClassNotFoundException {
        booleanObj.createBooleanTable();
    }

    @Test(priority = 0, enabled = true, dependsOnMethods = {"test00CreateBooleanTable"},
            description = "验证带有布尔类型字段的表的创建")
    public void test01BooleanFieldTableCreate() throws SQLException, ClassNotFoundException {
        String expectedTableName = booleanObj.getBooleanTableName().toUpperCase();
        List<String> actualTableList = getTableList();
        Assert.assertTrue(actualTableList.contains(expectedTableName));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01BooleanFieldTableCreate"},
            description = "验证true型数据插入和查询成功")
    public void test02trueValuesInsertAndQuery() throws SQLException {
        int actualInsertRows = booleanObj.insertTrueValues();
        Assert.assertEquals(actualInsertRows, 4);

        List<Boolean> expectedTrueList = new ArrayList<Boolean>();
        Boolean[] trueArray = new Boolean[] {true,true,true,true};
        for (int i=0; i < trueArray.length; i++){
            expectedTrueList.add(trueArray[i]);
        }
        System.out.println("Expected List: " + expectedTrueList);
        List<Boolean> actualTrueList = booleanObj.getTrueValues();
        System.out.println("Actual List: " + actualTrueList);

        Assert.assertEquals(actualTrueList, expectedTrueList);
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test02trueValuesInsertAndQuery"},
            description = "验证false型数据插入和查询成功")
    public void test03falseValuesInsertAndQuery() throws SQLException {
        int actualInsertRows = booleanObj.insertFalseValues();
        Assert.assertEquals(actualInsertRows, 4);

        List<Boolean> expectedFalseList = new ArrayList<Boolean>();
        Boolean[] falseArray = new Boolean[] {false,false,false,false};
        for (int i=0; i < falseArray.length; i++){
            expectedFalseList.add(falseArray[i]);
        }
        System.out.println("Expected List: " + expectedFalseList);
        List<Boolean> actualFalseList = booleanObj.getFalseValues();
        System.out.println("Actual List: " + actualFalseList);

        Assert.assertEquals(actualFalseList, expectedFalseList);
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test03falseValuesInsertAndQuery"},
            description = "验证布尔型数据插入和查询成功")
    public void test04trueAndfalseValuesInsertAndQuery() throws SQLException {
        int actualInsertRows = booleanObj.insertTrueAndFalseValues();
        Assert.assertEquals(actualInsertRows, 7);

        List<Boolean> expectedtrueAndfalseList = new ArrayList<Boolean>();
        Boolean[] trueAndfalseArray = new Boolean[] {true,false,true,false,false,true,true};
        for (int i=0; i < trueAndfalseArray.length; i++){
            expectedtrueAndfalseList.add(trueAndfalseArray[i]);
        }
        System.out.println("Expected List: " + expectedtrueAndfalseList);
        List<Boolean> actualtrueAndfalseList = booleanObj.getTrueAndFalseValues();
        System.out.println("Actual List: " + actualtrueAndfalseList);

        Assert.assertEquals(actualtrueAndfalseList, expectedtrueAndfalseList);
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按true值查询")
    public void test05TrueValueQuery() throws SQLException {
        List<String> expectedTrueValueConditionList = new ArrayList<String>();
        String[] expectedNameArray = new String[] {"zhangsan","lisi","lisi3","HAHA","oppo"," ab c d ","YH","yamaha"};
        for (int i=0; i < expectedNameArray.length; i++){
            expectedTrueValueConditionList.add(expectedNameArray[i]);
        }
        System.out.println("Expected List: " + expectedTrueValueConditionList);
        List<String> actualtrueValueConditionList = booleanObj.queryTrueValue();
        System.out.println("Actual List: " + actualtrueValueConditionList);

        Assert.assertEquals(actualtrueValueConditionList, expectedTrueValueConditionList);
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按is true查询")
    public void test05QueryIsTrue() throws SQLException {
        List<String> expectedIsTrueConditionList = new ArrayList<String>();
        String[] expectedNameArray = new String[] {"zhangsan","lisi","lisi3","HAHA","oppo"," ab c d ","YH","yamaha"};
        for (int i=0; i < expectedNameArray.length; i++){
            expectedIsTrueConditionList.add(expectedNameArray[i]);
        }
        System.out.println("Expected List: " + expectedIsTrueConditionList);
        List<String> actualIsTrueConditionList = booleanObj.queryIsTrue();
        System.out.println("Actual List: " + actualIsTrueConditionList);

        Assert.assertEquals(actualIsTrueConditionList, expectedIsTrueConditionList);
    }


    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按false值查询")
    public void test06FalseValueQuery() throws SQLException {
        List<Integer> expectedFalseValueConditionList = new ArrayList<Integer>();
        Integer[] expectedIDArray = new Integer[] {5, 6, 7, 8, 10, 12, 13};
        for (int i=0; i < expectedIDArray.length; i++){
            expectedFalseValueConditionList.add(expectedIDArray[i]);
        }
        System.out.println("Expected List: " + expectedFalseValueConditionList);
        List<Integer> actualfalseValueConditionList = booleanObj.queryFalseValue();
        System.out.println("Actual List: " + actualfalseValueConditionList);

        Assert.assertEquals(actualfalseValueConditionList, expectedFalseValueConditionList);
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按Is false查询")
    public void test06QueryIsFalse() throws SQLException {
        List<Integer> expectedIsFalseConditionList = new ArrayList<Integer>();
        Integer[] expectedIDArray = new Integer[] {5, 6, 7, 8, 10, 12, 13};
        for (int i=0; i < expectedIDArray.length; i++){
            expectedIsFalseConditionList.add(expectedIDArray[i]);
        }
        System.out.println("Expected List: " + expectedIsFalseConditionList);
        List<Integer> actualIsFalseConditionList = booleanObj.queryIsFalse();
        System.out.println("Actual List: " + actualIsFalseConditionList);

        Assert.assertEquals(actualIsFalseConditionList, expectedIsFalseConditionList);
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按字段为真查询")
    public void test07FieldAsConditionQuery() throws SQLException {
        List<Boolean> expectedFieldList = new ArrayList<Boolean>();
        Boolean[] expectedFieldArray = new Boolean[] {true,true,true,true,true,true,true,true};
        for (int i=0; i < expectedFieldArray.length; i++){
            expectedFieldList.add(expectedFieldArray[i]);
        }
        System.out.println("Expected List: " + expectedFieldList);
        List<Boolean> actualFieldList = booleanObj.fieldAsConditionValue();
        System.out.println("Actual List: " + actualFieldList);

        Assert.assertEquals(actualFieldList, expectedFieldList);
    }

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证按字段为假查询")
    public void test08NotFieldAsConditionQuery() throws SQLException {
        List<Boolean> expectedNotFieldList = new ArrayList<Boolean>();
        Boolean[] expectedNotFieldArray = new Boolean[] {false,false,false,false,false,false,false};
        for (int i=0; i < expectedNotFieldArray.length; i++){
            expectedNotFieldList.add(expectedNotFieldArray[i]);
        }
        System.out.println("Expected List: " + expectedNotFieldList);
        List<Boolean> actualNotFieldList = booleanObj.notFieldAsConditionValue();
        System.out.println("Actual List: " + actualNotFieldList);

        Assert.assertEquals(actualNotFieldList, expectedNotFieldList);
    }


    @Test(priority = 8, enabled = true, expectedExceptions = SQLException.class, dataProvider = "yamlBooleanMethod",
            dependsOnMethods = {"test08NotFieldAsConditionQuery"}, description = "预期插入失败")
    public void test09InsertStrValue(Map<String, String> param) throws SQLException {
        String booleanTable = BooleanField.getBooleanTableName();
        try(Statement statement = BooleanField.connection.createStatement()) {
            String insertSql = "insert into " + booleanTable + " values (" + param.get("ID") +
                    ",'vivo',20,456.7,'shanghai','" + param.get("booleanValue") + "')";
            statement.execute(insertSql);
        }
    }

    @Test(priority = 9, enabled = true, expectedExceptions = SQLException.class, dataProvider = "yamlBooleanMethod",
            dependsOnMethods = {"test09InsertStrValue"}, description = "预期插入失败")
    public void test10InsertWrongValue(Map<String, String> param) throws SQLException {
        String booleanTable = BooleanField.getBooleanTableName();
        try(Statement statement = BooleanField.connection.createStatement()) {
            String insertSql = "insert into " + booleanTable + " values (" + param.get("ID") +
                    ",'vivo',20,456.7,'shanghai'," + param.get("booleanValue") + ")";
            statement.execute(insertSql);
        }
    }


    @Test(priority = 10, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            description = "验证插入0，转换为False")
    public void test11ZeroValueQuery() throws SQLException {
        int actualInsertRows = booleanObj.insertZeroValues();
        Assert.assertEquals(actualInsertRows, 1);
        Boolean actualZeroValue = booleanObj.getZeroValues();
        System.out.println("Actual: " + actualZeroValue);

        Assert.assertFalse(actualZeroValue);
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"},
            dataProvider = "yamlBooleanMethod", description = "验证插入正整数，转换为True")
    public void test12IntegerValueQuery(Map<String, String> param) throws SQLException {
        int actualInsertRows = booleanObj.insertPosIntegerValues(param.get("ID"), param.get("booleanValue"));
        Assert.assertEquals(actualInsertRows, 1);
        Boolean actualIntegerValue = booleanObj.getIntegerValues(param.get("ID"));
        System.out.println("Actual: " + actualIntegerValue);

        Assert.assertTrue(actualIntegerValue);
    }

    @Test(priority = 12, enabled = true, expectedExceptions = SQLException.class,
            description = "验证字段类型为bool，创建失败")
    public void test13CreateTableUsingBoolFailed() throws SQLException, ClassNotFoundException {
        booleanObj.createBoolTable();
    }

    @AfterClass (alwaysRun = true, description = "执行测试后删除数据，删除表")
    public void teardownAll() {
        String booleanTable = BooleanField.getBooleanTableName();
        Statement tearDownStatement = null;
        try {
            tearDownStatement = BooleanField.connection.createStatement();
            tearDownStatement.execute("delete from " + booleanTable);
            tearDownStatement.execute("drop table " + booleanTable);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(tearDownStatement != null){
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if(BooleanField.connection != null){
                    BooleanField.connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
