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

package io.dingodb.test.compositedatatype;

import io.dingodb.common.utils.JDBCUtils;
import io.dingodb.dailytest.compositedatatype.ArrayField;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.StrTo2DList;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class TestArrayField extends YamlDataHelper {
    public static ArrayField arrayObj = new ArrayField();

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(arrayObj.connection);
    }

    @Test(priority = 1, enabled = true, dataProvider = "arrayFieldMethod",
            description = "创建含有不同数据类型的array字段的数据表，不指定默认值")
    public void test01TableCreateWithArrayField(Map<String, String> param) throws SQLException {
        arrayObj.tableCreateWithArrayField(param.get("tableName"), param.get("fieldName"), param.get("fieldType"));
    }

    @Test(priority = 2, enabled = true, dataProvider = "arrayFieldMethod",dependsOnMethods = {"test01TableCreateWithArrayField"},
            description = "向不同数据类型的array字段的数据表中插入数据")
    public void test02InsertArrayValues(Map<String, String> param) throws SQLException {
        arrayObj.insertArrayValues(param.get("tableName"), param.get("arrayValues"));
    }

    @Test(priority = 3, enabled = true, dataProvider = "arrayFieldMethod",dependsOnMethods = {"test02InsertArrayValues"},
            description = "验证插入后的array值显示正确")
    public void test02QueryArrayData(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);
        List actualList = arrayObj.queryTableData(param.get("tableName"));
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }


    @Test(priority = 4, enabled = true, dataProvider = "arrayFieldMethod",
            description = "创建含有不同数据类型的array字段的数据表,指定默认值")
    public void test03TableCreateWithArrayFieldDefaultValue(Map<String, String> param) throws SQLException {
        arrayObj.tableCreateWithArrayFieldDefaultValue(param.get("tableName"), param.get("fieldName"), param.get("fieldType"), param.get("defaultValue"));
    }

    @Test(priority = 5, enabled = true, dataProvider = "arrayFieldMethod",
            description = "向不同数据表插入除数组类型字段外的其他字段值")
    public void test04InsertValuesWithArrayField(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List expectedList = strTo2DList.construct1DList(param.get("outData"), ";");
        System.out.println("Expected: " + expectedList);
        List actualList = arrayObj.insertValuesWithoutArrayField(param.get("tableName"), param.get("typeFields"), param.get("insertValue"));
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try{
            tearDownStatement = arrayObj.connection.createStatement();
            tearDownStatement.execute("delete from intArray");
            tearDownStatement.execute("drop table intArray");
            tearDownStatement.execute("delete from bigintArray");
            tearDownStatement.execute("drop table bigintArray");
            tearDownStatement.execute("delete from varcharArray");
            tearDownStatement.execute("drop table varcharArray");
            tearDownStatement.execute("delete from charArray");
            tearDownStatement.execute("drop table charArray");
            tearDownStatement.execute("delete from doubleArray");
            tearDownStatement.execute("drop table doubleArray");
            tearDownStatement.execute("delete from floatArray");
            tearDownStatement.execute("drop table floatArray");
            tearDownStatement.execute("delete from dateArray");
            tearDownStatement.execute("drop table dateArray");
            tearDownStatement.execute("delete from timeArray");
            tearDownStatement.execute("drop table timeArray");
            tearDownStatement.execute("delete from timestampArray");
            tearDownStatement.execute("drop table timestampArray");
            tearDownStatement.execute("delete from boolArray");
            tearDownStatement.execute("drop table boolArray");
            tearDownStatement.execute("delete from intArrayDefault");
            tearDownStatement.execute("drop table intArrayDefault");
            tearDownStatement.execute("delete from varcharArrayDefault");
            tearDownStatement.execute("drop table varcharArrayDefault");
            tearDownStatement.execute("delete from doubleArrayDefault");
            tearDownStatement.execute("drop table doubleArrayDefault");
            tearDownStatement.execute("delete from dateArrayDefault");
            tearDownStatement.execute("drop table dateArrayDefault");
            tearDownStatement.execute("delete from timeArrayDefault");
            tearDownStatement.execute("drop table timeArrayDefault");
            tearDownStatement.execute("delete from timestampArrayDefault");
            tearDownStatement.execute("drop table timestampArrayDefault");
            tearDownStatement.execute("delete from boolArrayDefault");
            tearDownStatement.execute("drop table boolArrayDefault");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(arrayObj.connection, tearDownStatement);
        }
    }
}
