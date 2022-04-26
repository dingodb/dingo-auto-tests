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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestBooleanField {
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

    @Test(priority = 0, enabled = true, description = "验证带有布尔类型字段的表的创建")
    public void test01BooleanFieldTableCreate() throws SQLException, ClassNotFoundException {
        booleanObj.createBooleanTable();
        String expectedTableName = booleanObj.getBooleanTableName().toUpperCase();
        List<String> actualTableList = getTableList();
        Assert.assertTrue(actualTableList.contains(expectedTableName));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01BooleanFieldTableCreate"}, description = "验证true型数据插入和查询成功")
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

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test02trueValuesInsertAndQuery"}, description = "验证false型数据插入和查询成功")
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

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test03falseValuesInsertAndQuery"}, description = "验证布尔型数据插入和查询成功")
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

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"}, description = "验证按true值查询")
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

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"}, description = "验证按false值查询")
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

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"}, description = "验证按字段为真查询")
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

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test04trueAndfalseValuesInsertAndQuery"}, description = "验证按字段为假查询")
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

    @AfterClass (alwaysRun = true, description = "执行测试后删除数据，删除表")
    public void teardownAll() throws SQLException {
        String booleanTable = BooleanField.getBooleanTableName();
        Statement tearDownStatement = BooleanField.connection.createStatement();
        tearDownStatement.execute("delete from " + booleanTable);
        tearDownStatement.execute("drop table " + booleanTable);
        tearDownStatement.close();
        BooleanField.connection.close();
    }
}
