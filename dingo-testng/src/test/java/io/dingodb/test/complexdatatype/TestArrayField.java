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

package io.dingodb.test.complexdatatype;

import io.dingodb.common.utils.JDBCUtils;
import io.dingodb.dailytest.complexdatatype.ArrayField;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.StrTo2DList;
import utils.YamlDataHelper;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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

    @Test(priority = 2, enabled = true, dataProvider = "arrayFieldMethod",
            description = "向不同数据类型的array字段的数据表中插入数据")
    public void test02InsertArrayValues(Map<String, String> param) throws SQLException {
        arrayObj.insertArrayValues(param.get("tableName"), param.get("arrayValues"));
    }

    @Test(priority = 3, enabled = true, dataProvider = "arrayFieldMethod", description = "验证插入后的array值显示正确")
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
            description = "向不同数据表插入除数组类型字段外的其他字段值,并验证插入后的数据")
    public void test04InsertValuesWithArrayField(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List expectedList = strTo2DList.construct1DList(param.get("outData"), ";");
        System.out.println("Expected: " + expectedList);
        List actualList = arrayObj.insertValuesWithoutArrayField(param.get("tableName"), param.get("typeFields"), param.get("insertValue"));
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 6, enabled = true, dataProvider = "arrayFieldMethod",
            description = "插入Array类型字段Null值")
    public void test05QueryWithArrayColNull(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        arrayObj.insertArrayValues(param.get("tableName"), param.get("arrayValues"));

        List<List> actualList = arrayObj.queryDataWithCondition(param.get("tableName"), param.get("queryLogic"));
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 7, enabled = true, dataProvider = "arrayFieldMethod", expectedExceptions = SQLException.class,
            description = "插入混合类型，非法的时间日期，以及空参，预期失败")
    public void test06InsertMixedAndIllegalElement(Map<String, String> param) throws SQLException {
        arrayObj.insertArrayValues(param.get("tableName"), param.get("arrayValues"));
    }

    @Test(priority = 8, enabled = true, dataProvider = "arrayFieldMethod",
            description = "插入单一类型成功，以及不同时间日期格式的支持")
    public void test07InsertSingleElementTypeSuccess(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        arrayObj.insertArrayValues(param.get("tableName"), param.get("arrayValues"));

        List<List> actualList = arrayObj.queryDataWithCondition(param.get("tableName"), param.get("queryLogic"));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 9, enabled = true, description = "创建含有多种Array类型字段的表")
    public void test08TableCreateWithMixArrayColumn() throws SQLException {
        String mixarray_meta1_path = "src/test/resources/testdata/tablemeta/complexdatatype/array_tbl1_meta.txt";
        String mixarray_meta1 = FileReaderUtil.readFile(mixarray_meta1_path);
        String tableName = "mixarray1";
        arrayObj.tableCreateWithMixArrayColumn(tableName, mixarray_meta1);
    }

    @Test(priority = 10, enabled = true, description = "向含有多种Array类型字段的表插入数据")
    public void test09InsertValuesToTableWithMixArrayColumn() throws SQLException {
        String mixarray_value1_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl1_value1.txt";
        String mixarray_value1 = FileReaderUtil.readFile(mixarray_value1_path);
        String tableName = "mixarray1";
        int actualRows1 = arrayObj.insertValuesToMixArrayTable(tableName, mixarray_value1);
        Assert.assertEquals(actualRows1, 1);

        String mixarray_value2_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl1_value2.txt";
        String mixarray_value2 = FileReaderUtil.readFile(mixarray_value2_path);
        int actualRows2 = arrayObj.insertValuesToMixArrayTable(tableName, mixarray_value2);
        Assert.assertEquals(actualRows2, 2);
    }

    @Test(priority = 11, enabled = true, expectedExceptions = SQLException.class, description = "验证array字段值不支持作为条件进行查询")
    public void test10QueryByArrayValueFail() throws SQLException {
        arrayObj.queryByArrayValue();
    }

    @Test(priority = 12, enabled = true, description = "删除含有array类型字段的单条数据")
    public void test11DeleteSingleRecordWithArrayColumn() throws SQLException {
        String conditionStr = " where id=3";
        int deleteRows = arrayObj.deleteSingleRecordWithArrayColumn(conditionStr);
        Assert.assertEquals(deleteRows, 1);
    }

    @Test(priority = 13, enabled = true, description = "删除含有array类型字段的全表数据")
    public void test12DeleteAllRecordWithArrayColumn() throws SQLException {
        int deleteRows = arrayObj.deleteSingleRecordWithArrayColumn("");
        Assert.assertEquals(deleteRows, 2);

        Assert.assertFalse(arrayObj.queryAfterDeleteAll());
    }

    @Test(priority = 14, enabled = true, description = "创建表，数组类型列位于表中间列，主键在最后列")
    public void test13TableCreateArrayColumnInMid() throws SQLException {
        String arraymid_meta_path = "src/test/resources/testdata/tablemeta/complexdatatype/array_tbl2_meta.txt";
        String arraymid_meta = FileReaderUtil.readFile(arraymid_meta_path);
        String tableName = "arraymid";
        arrayObj.tableCreateWithMixArrayColumn(tableName, arraymid_meta);
    }

    @Test(priority = 15, enabled = true, description = "向array列在中间列的表中插入数据")
    public void test14InsertValuesToTableWithArrayColumnMid() throws SQLException {
        String arraymid_value1_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl2_value1.txt";
        String arraymid_value1 = FileReaderUtil.readFile(arraymid_value1_path);
        String tableName = "arraymid";
        int actualRows1 = arrayObj.insertValuesToMixArrayTable(tableName, arraymid_value1);
        Assert.assertEquals(actualRows1, 1);

        String arraymid_value2_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl2_value2.txt";
        String arraymid_value2 = FileReaderUtil.readFile(arraymid_value2_path);
        int actualRows2 = arrayObj.insertValuesToMixArrayTable(tableName, arraymid_value2);
        Assert.assertEquals(actualRows2, 2);
    }

    @Test(priority = 16, enabled = true, description = "创建表，数组类型列位于表首列，主键在中间列")
    public void test15TableCreateArrayColumnFirst() throws SQLException {
        String arrayfirst_meta_path = "src/test/resources/testdata/tablemeta/complexdatatype/array_tbl3_meta.txt";
        String arrayfirst_meta = FileReaderUtil.readFile(arrayfirst_meta_path);
        String tableName = "arrayfirst";
        arrayObj.tableCreateWithMixArrayColumn(tableName, arrayfirst_meta);
    }

    @Test(priority = 17, enabled = true, description = "向array列在首列的表中插入数据")
    public void test16InsertValuesToTableWithArrayColumnFirst() throws SQLException {
        String arrayfirst_value1_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl3_value1.txt";
        String arrayfirst_value1 = FileReaderUtil.readFile(arrayfirst_value1_path);
        String tableName = "arrayfirst";
        int actualRows1 = arrayObj.insertValuesToMixArrayTable(tableName, arrayfirst_value1);
        Assert.assertEquals(actualRows1, 1);

        String arrayfirst_value2_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl3_value2.txt";
        String arrayfirst_value2 = FileReaderUtil.readFile(arrayfirst_value2_path);
        int actualRows2 = arrayObj.insertValuesToMixArrayTable(tableName, arrayfirst_value2);
        Assert.assertEquals(actualRows2, 3);
    }


    @Test(priority = 18, enabled = true, dataProvider = "arrayFieldMethod", description = "查询array字段在中间和开头的表数据")
    public void test17QueryInMidAndFirst(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        File file = new File("src/test/resources/testdata/tablemeta/complexdatatype/array_tbl3_meta.txt");
        int ArrayFirstFieldNum = FileReaderUtil.getFileLines(file) - 3;
        System.out.println(ArrayFirstFieldNum);
        List<List> actualList = arrayObj.queryDataInMidFirstArrayTbl(param.get("queryFields"),param.get("tableName"),param.get("queryLogic"), ArrayFirstFieldNum);
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 19, enabled = true, description = "array字段允许为null,插入数据不指定该字段")
    public void test18InsertToTableWithoutArrayNull() throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        String parseStr = "1&zhangsan&55&23.45&null;";
        List<List> expectedList = strTo2DList.construct2DList(parseStr, ";", "&");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = arrayObj.insertToTableWithoutArrayColAllowNull();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 20, enabled = true, expectedExceptions = SQLException.class, description = "array字段不允许为null,插入数据不指定该字段，预期失败")
    public void test19InsertToTableWithoutArrayNotNull() throws SQLException {
        arrayObj.insertToTableWithoutArrayColNotNull();
    }

    @Test(priority = 21, enabled = true, description = "用于范围查询的表创建和插入")
    public void test20RangeQuery() throws SQLException {
        String array_meta4_path = "src/test/resources/testdata/tablemeta/complexdatatype/array_tbl4_meta.txt";
        String array_meta4 = FileReaderUtil.readFile(array_meta4_path);
        String tableName = "arrayrangetest";
        arrayObj.tableCreateWithMixArrayColumn(tableName, array_meta4);

        String array_value4_path = "src/test/resources/testdata/tableInsertValues/complexdatatype/array_tbl4_value1.txt";
        String array_value4 = FileReaderUtil.readFile(array_value4_path);
        int actualRows = arrayObj.insertValuesToMixArrayTable(tableName, array_value4);
        Assert.assertEquals(actualRows, 5);

    }

    @Test(priority = 22, enabled = true, dataProvider = "arrayFieldMethod", description = "范围查询")
    public void test21RangeQuery(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);
        File file = new File("src/test/resources/testdata/tablemeta/complexdatatype/array_tbl4_meta.txt");
        int ArrayTableFieldNum = FileReaderUtil.getFileLines(file) - 3;
        List<List> actualList = arrayObj.queryDataInMidFirstArrayTbl(param.get("queryFields"),param.get("tableName"),param.get("queryLogic"), ArrayTableFieldNum);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 23, enabled = true, dataProvider = "arrayFieldMethod",
            description = "删除array字段值为null或者元素含有null的行")
    public void test22DeleteRecordsWithArrayNull(Map<String, String> param) throws SQLException {
        int deleteRows = arrayObj.deleteArrayNullRecords(param.get("tableName"),param.get("deleteLogic"));
        System.out.println("Actual: " + deleteRows);
        Assert.assertEquals(deleteRows, Integer.parseInt(param.get("recordDel")));
    }

    @Test(priority = 24, enabled = true, description = "删除array字段值为null或者元素含有null的行后查询数据")
    public void test23QueryAfterDeleteNull() throws SQLException {
        boolean actualRows = arrayObj.queryAfterDeleteNull();
        Assert.assertFalse(actualRows);
    }

    @Test(priority = 25, enabled = true, description = "指定Array字段插入数据")
    public void test24InsertSpecifyArrayCol() throws SQLException {
        List expectedList = new ArrayList();
        expectedList.add("2186");
        expectedList.add(null);
        expectedList.add(null);
        expectedList.add(null);
        expectedList.add("[test1, hg, 177]");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = arrayObj.insertToTableWithArrayColSpecified();
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 26, enabled = true, dataProvider = "arrayFieldMethod", description = "更新array列数组")
    public void test25UpdateArrayAndQuery(Map<String, String> param) throws SQLException {
        int expectedAffectRows = Integer.parseInt(param.get("affectRows"));
        System.out.println("Expected affected rows: " + expectedAffectRows);
        int updateRows = arrayObj.arrayUpdate(param.get("updateState"));
        System.out.println("Actual update rows: " + updateRows);

        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        List<List> actualList = arrayObj.queryAfterArrayUpdate(param.get("queryState"));
        System.out.println("Actual；" + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();
        try{
            tearDownStatement = arrayObj.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
//            tearDownStatement.execute("drop table intArray");
//            tearDownStatement.execute("drop table bigintArray");
//            tearDownStatement.execute("drop table varcharArray");
//            tearDownStatement.execute("drop table charArray");
//            tearDownStatement.execute("drop table doubleArray");
//            tearDownStatement.execute("drop table floatArray");
//            tearDownStatement.execute("drop table dateArray");
//            tearDownStatement.execute("drop table timeArray");
//            tearDownStatement.execute("drop table timestampArray");
//            tearDownStatement.execute("drop table boolArray");
//            tearDownStatement.execute("drop table intArrayDefault");
//            tearDownStatement.execute("drop table varcharArrayDefault");
//            tearDownStatement.execute("drop table doubleArrayDefault");
//            tearDownStatement.execute("drop table dateArrayDefault");
//            tearDownStatement.execute("drop table timeArrayDefault");
//            tearDownStatement.execute("drop table timestampArrayDefault");
//            tearDownStatement.execute("drop table boolArrayDefault");
//            tearDownStatement.execute("drop table atest1903");
//            tearDownStatement.execute("drop table mixarray1");
//            tearDownStatement.execute("drop table arraymid");
//            tearDownStatement.execute("drop table arrayfirst");
//            tearDownStatement.execute("drop table arrayNu");
//            tearDownStatement.execute("drop table arrayNotNu");
//            tearDownStatement.execute("drop table arrayrangetest");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(arrayObj.connection, tearDownStatement);
        }
    }
}
