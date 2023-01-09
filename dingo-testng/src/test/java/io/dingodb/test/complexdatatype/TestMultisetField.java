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
import io.dingodb.dailytest.complexdatatype.MultisetField;
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

public class TestMultisetField extends YamlDataHelper {
    public static MultisetField multisetObj = new MultisetField();
    public static String mixTableName = "mixset1";
    public static String tableName1 = "settest1";
    public static String tableName2 = "settest2";
    public static String tableName3 = "settest3";

    public void initMultisetTable(String tableName, String tableMetaPath) throws SQLException {
        String multiset_meta = FileReaderUtil.readFile(tableMetaPath);
        multisetObj.tableCreateWithMultisetField(tableName, multiset_meta);
    }

    public void insertValueToTable(String tableName, String insertField, String tableValuePath) throws SQLException {
        String multiset_value = FileReaderUtil.readFile(tableValuePath);
        multisetObj.insertMultisetValues(tableName, insertField, multiset_value);
    }

    public static List<List> expectedMultisetOutput(String[][] dataArray) {
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(multisetObj.connection);
    }

    @Test(priority = 1, enabled = true, dataProvider = "multisetFieldMethod",
            description = "创建含有不同数据类型的multiset字段的数据表，不指定默认值和指定默认值")
    public void test01TableCreateWithMultisetField(Map<String, String> param) throws SQLException {
        multisetObj.tableCreateWithMultisetField(param.get("tableName"), param.get("tableMeta"));
    }

    @Test(priority = 2, enabled = true, dataProvider = "multisetFieldMethod",
            description = "向不同数据类型的multiset字段的数据表中插入数据")
    public void test02InsertMultisetValues(Map<String, String> param) throws SQLException {
        multisetObj.insertMultisetValues(param.get("tableName"), param.get("insertFields"), param.get("tableValue"));
    }

    @Test(priority = 3, enabled = true, dataProvider = "multisetFieldMethod", description = "验证插入后的multiset值显示正确")
    public void test03QueryMultisetData(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);
        List actualList = multisetObj.queryTableData(param.get("tableName"), param.get("queryFields"), "", Integer.parseInt(param.get("fieldsCnt")));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 4, enabled = true, dataProvider = "multisetFieldMethod",
            description = "插入Multiset类型字段Null值,并查询")
    public void test04InsertMultisetColumnNull(Map<String, String> param) throws SQLException {
        switch (param.get("tableName")) {
            case "intset": {
                String[][] dataArray1 = {
                        {"20251","zhangsan",null}
                };
                List<List> expectedList = expectedMultisetOutput(dataArray1);
                System.out.println("Expected: " + expectedList);
                multisetObj.insertMultisetValues(param.get("tableName"), "", param.get("tableValue"));
                List<List> actualList = multisetObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryLogic"), Integer.parseInt(param.get("fieldsCnt")));
                System.out.println("Actual: " + actualList);
                Assert.assertEquals(actualList, expectedList);
                break;
            }

            case "varcharset": {
                String[][] dataArray1 = {
                        {"20252","lisi",null}
                };
                List<List> expectedList = expectedMultisetOutput(dataArray1);
                System.out.println("Expected: " + expectedList);
                multisetObj.insertMultisetValues(param.get("tableName"), "", param.get("tableValue"));
                List<List> actualList = multisetObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryLogic"), Integer.parseInt(param.get("fieldsCnt")));
                System.out.println("Actual: " + actualList);
                Assert.assertEquals(actualList, expectedList);
                break;
            }
        }
    }

    @Test(priority = 5, enabled = true, dataProvider = "multisetFieldMethod", expectedExceptions = SQLException.class,
            description = "插入multiset元素为Null或其他不支持的混合元素，预期失败用例")
    public void test05MultisetValueNotSupport(Map<String, String> param) throws SQLException {
        multisetObj.insertMultisetValues(param.get("tableName"), "", param.get("tableValue"));
    }

    @Test(priority = 6, enabled = true, dataProvider = "multisetFieldMethod",
            description = "插入混合类型元素值，预期成功用例")
    public void test06MixValuesSupport(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        multisetObj.insertMultisetValues(param.get("tableName"), param.get("insertFields"), param.get("tableValue"));
        List<List> actualList = multisetObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryLogic"), Integer.parseInt(param.get("fieldsCnt")));
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 7, enabled = true, description = "创建含有多个Multiset类型字段的数据表，不指定默认值")
    public void test07TableCreateWithMultiMultisetFields() throws SQLException {
        String multiset_mix_meta_path = "src/test/resources/tabledata/meta/complexdatatype/multiset/multiset_mix_meta.txt";
        initMultisetTable(mixTableName, multiset_mix_meta_path);
    }

    @Test(priority = 8, enabled = true, description = "向含有多个Multiset类型字段的表中插入数据")
    public void test08InsertValuesToMultiMapFields() throws SQLException {
        String multiset_mix_value1_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_mix_value1.txt";
        insertValueToTable(mixTableName,"", multiset_mix_value1_path);

        String multiset_mix_value2_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_mix_value2.txt";
        insertValueToTable(mixTableName,"", multiset_mix_value2_path);

        String[][] dataArray = {
                {"1", "zhangsan", "18", "[10, 200, 3000]", "[male, BJHD, 1901]", "[2345.78, 24.0, 0.001]", "[2022-08-18, 2022-08-19]", "[00:00:00, 12:23:34, 23:59:59]", "[2008-10-31 15:38:02]", "[true, false, true, false, true, true]"},
                {"2", "lisi", "25", "[99999999]", "[female]", "[0.0]", "[2022-01-01]", "[03:02:01]", "[2028-10-31 15:38:02]", "[false]"},
                {"3", "lisi", "25", "[99999999]", "[female]", "[0.0]", "[2022-01-01]", "[03:02:01]", "[2028-10-31 15:38:02]", "[false]"},
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryState = " where id in (1,2,3)";
        List<List> actualList = multisetObj.queryTableData(mixTableName, "*", queryState, 10);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 9, enabled = true, expectedExceptions = SQLException.class, description = "验证multiset字段值不支持作为条件进行查询")
    public void test09QueryByMultisetValueFail() throws SQLException {
        String queryFields = "*";
        String queryState = " where class_no=multiset[10, 200, 3000]";
        multisetObj.queryTableData(mixTableName, queryFields, queryState, 10);
    }

    @Test(priority = 10, enabled = false, dataProvider = "multisetFieldMethod", description = "更新multiset列值")
    public void test10UpdateMultisetAndQuery(Map<String, String> param) throws SQLException {
        int expectedAffectRows = Integer.parseInt(param.get("affectRows"));
        System.out.println("Expected affected rows: " + expectedAffectRows);
        int updateRows = multisetObj.multisetUpdate(param.get("updateState"));
        System.out.println("Actual update rows: " + updateRows);

        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"), ";", "&");
        System.out.println("Expected: " + expectedList);

        List<List> actualList = multisetObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryLogic"), Integer.parseInt(param.get("fieldsCnt")));
        System.out.println("Actual；" + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 11, enabled = true, description = "删除含有multiset类型字段的单条数据")
    public void test11DeleteSingleRecordWithMultiset() throws SQLException {
        String conditionStr = " where id=3";
        int deleteRows = multisetObj.deleteTableData(mixTableName, conditionStr);
        Assert.assertEquals(deleteRows, 1);

        String[][] dataArray = {
                {"1", "zhangsan", "18", "[10, 200, 3000]", "[male, BJHD, 1901]", "[2345.78, 24.0, 0.001]", "[2022-08-18, 2022-08-19]", "[00:00:00, 12:23:34, 23:59:59]", "[2008-10-31 15:38:02]", "[true, false, true, false, true, true]"},
                {"2", "lisi", "25", "[99999999]", "[female]", "[0.0]", "[2022-01-01]", "[03:02:01]", "[2028-10-31 15:38:02]", "[false]"}
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected after delete: " + expectedList);
        String queryState = " where id in (1,2,3)";
        List<List> actualList = multisetObj.queryTableData(mixTableName, "*", queryState, 10);
        System.out.println("Actual after delete: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 12, enabled = true, description = "删除含有multiset类型字段的全表数据")
    public void test12DeleteAllRecordWithMultiset() throws SQLException {
        int deleteRows = multisetObj.deleteTableData(mixTableName, "");
        Assert.assertEquals(deleteRows, 2);
        Assert.assertFalse(multisetObj.queryResultSet(mixTableName, "*", ""));
    }

    @Test(priority = 13, enabled = true, description = "multiset字段允许为Null, 不指定multiset字段插入")
    public void test13InsertWithoutFieldsSpecified() throws SQLException {
        String multiset_allownull_meta_path = "src/test/resources/tabledata/meta/complexdatatype/multiset/multiset_allownull_meta.txt";
        String multiset_allownull_value_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_allownull_value.txt";
        initMultisetTable(tableName1, multiset_allownull_meta_path);
        String insertFields = "(id,name,age)";
        insertValueToTable(tableName1, insertFields, multiset_allownull_value_path);
        String[][] dataArray = {
                {"1", "zhangsan", "55", null}
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryState = " where id = 1";
        List<List> actualList = multisetObj.queryTableData(tableName1, "*", queryState,4);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 14, enabled = true, expectedExceptions = SQLException.class,
            description = "创建表，multiset类型不允许为null，插入数据不指定multiset字段，预期失败")
    public void test14TableCreateMultisetNotNull() throws SQLException {
        String multiset_notallownull_meta_path = "src/test/resources/tabledata/meta/complexdatatype/multiset/multiset_notallownull_meta.txt";
        initMultisetTable(tableName2, multiset_notallownull_meta_path);
        String multiset_notallownull_value_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_notallownull_value.txt";
        String insertFields = "(id,name,age)";
        insertValueToTable(tableName2, insertFields, multiset_notallownull_value_path);
    }

    @Test(priority = 15, enabled = true, description = "multiset字段表范围查询1")
    public void test15RangeQuery1() throws SQLException {
        String multiset_rangequery_meta_path = "src/test/resources/tabledata/meta/complexdatatype/multiset/multiset_rangequery_meta.txt";
        String multiset_rangequery_value_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_rangequery_value.txt";
        initMultisetTable(tableName3, multiset_rangequery_meta_path);
        insertValueToTable(tableName3, "", multiset_rangequery_value_path);
        String[][] dataArray = {
                {"-1230.44", "[male1234, shanghai]"},
                {"99.9", "[female, Jinan3, 789.23]"}
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "amount,user_info";
        String queryState = " where id > 1 and age < 30";
        List<List> actualList = multisetObj.queryTableData(tableName3, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 15, enabled = true, description = "multiset字段表范围查询2")
    public void test15RangeQuery2() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "[male000, beijing, 1234.56]"},
                {"2", "lisi", "[female]"},
                {"3", "wangwu", "[male1234, shanghai]"}
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "id,name,user_info";
        String queryState = " where id < 3 or amount < 0";
        List<List> actualList = multisetObj.queryTableData(tableName3, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 16, enabled = true, description = "删除整型列值为null的行数据，预期可删除成功")
    public void test16DeleteRowWithNull1() throws SQLException {
        String tableName = "intset";
        String queryFields = "*";
        String queryState = " where id = 20251";
        int actualDelete = multisetObj.deleteTableData(tableName, queryState);
        System.out.println("Actual delete " + actualDelete + " rows");
        Assert.assertEquals(actualDelete, 1);

        Boolean actualResult = multisetObj.queryResultSet(tableName, queryFields, queryState);
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(priority = 16, enabled = true, description = "删除字符型列值为null的行数据，预期可删除成功")
    public void test16DeleteRowWithNull2() throws SQLException {
        String tableName = "varcharset";
        String queryFields = "*";
        String queryState = " where id = 20252";
        int actualDelete = multisetObj.deleteTableData(tableName, queryState);
        System.out.println("Actual delete " + actualDelete + " rows");
        Assert.assertEquals(actualDelete, 1);

        Boolean actualResult = multisetObj.queryResultSet(tableName, queryFields, queryState);
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(priority = 147, enabled = true, description = "指定multiset字段插入")
    public void test17InsertWithFieldsSpecified() throws SQLException {
        String multiset_fieldinsert_value_path = "src/test/resources/tabledata/value/complexdatatype/multiset/multiset_fieldinsert_value.txt";
        String insertFields = "(id,user_info)";
        insertValueToTable(tableName3, insertFields, multiset_fieldinsert_value_path);
        String[][] dataArray = {
                {"2187", null, null, null, "[te1, hg2, 177]"}
        };
        List<List> expectedList = expectedMultisetOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 2187";
        List<List> actualList = multisetObj.queryTableData(tableName3, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList(
                "intset", "bigintset", "varcharset", "charset", "doubleset",
                "floatset", "dateset", "timeset", "timestampset", "boolset",
                "intsetdefault", "varcharsetdefault", "doublesetdefault", "datesetdefault", "timesetdefault",
                "timestampsetdefault", "boolsetdefault", "setmid", "setfirst", "mixset1",
                "settest1", "settest2", "settest3"
        );
        try{
            tearDownStatement = multisetObj.connection.createStatement();
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
        }finally {
            JDBCUtils.closeResource(multisetObj.connection, tearDownStatement);
        }
    }
}

