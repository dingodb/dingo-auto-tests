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
import io.dingodb.dailytest.complexdatatype.MapField;
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
import java.util.List;
import java.util.Map;

public class TestMapField extends YamlDataHelper {
    public static MapField mapObj = new MapField();
    public static String tableName1 = "maptest1";
    public static String tableName2 = "maptest2";
    public static String tableName3 = "maptest3";
    public static String tableName4 = "maptestdefault";
    public static String tableName5 = "mapmid";
    public static String tableName6 = "mapfirst";

    public void initMapTable(String tableName, String tableMetaPath) throws SQLException {
        String map_meta = FileReaderUtil.readFile(tableMetaPath);
        mapObj.tableCreateWithMapField(tableName, map_meta);
    }

    public void insertKVToTbl(String tableName, String insertField, String mapValuePath) throws SQLException {
        String map_value = FileReaderUtil.readFile(mapValuePath);
        mapObj.insertKV(tableName, insertField, map_value);
    }

    public static List<List> expectedMapOutput(String[][] dataArray) {
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
        Assert.assertNotNull(mapObj.connection);
    }

    @Test(priority = 1, enabled = true, dataProvider = "mapFieldMethod", description = "创建测试用表")
    public void test01TableCreateWithMapField(Map<String, String> param) throws SQLException {
        String map_meta_path = param.get("metaPath");
        initMapTable(param.get("tableName"), map_meta_path);
    }

    @Test(priority = 2, enabled = true, description = "插入字符型键值")
    public void test02InsertVarcharKV() throws SQLException {
        String map1_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value1.txt";
        insertKVToTbl(tableName1,"", map1_value1_path);
        String[][] dataArray = {
                {"1", "zhangsan", "20", "34.56", "{address=beijing, class_no=1024}"},
                {"2", "lisi", "35", "0.96", "{address=tianjin}"},
                {"3", "wangwu", "8", "0.0", "{address=dalian, sex=male}"},
                {"4", "liuyi", "-56", "-1834.29", "{address=wuhan, class_no=1001, sex=female}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id < 5";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 3, enabled = true, description = "插入时间日期型键值")
    public void test03InsertDateTimeKV() throws SQLException {
        String map1_value2_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value2.txt";
        insertKVToTbl(tableName1,"", map1_value2_path);
        String[][] dataArray = {
                {"{birthday=1989-06-17, create_time=14:55:35, join_time=2022-08-24 14:41:00}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "user_info";
        String queryState = " where id = 5";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 4, enabled = true, description = "插入整型键值")
    public void test04InsertIntKV() throws SQLException {
        String map1_value3_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value3.txt";
        insertKVToTbl(tableName1,"", map1_value3_path);
        String[][] dataArray = {
                {"6", "zhangsan", "20", "34.56", "{new_cnt=7829, price=100, uid=385671}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "id,name,age,amount,user_info";
        String queryState = " where id = 6";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 5, enabled = true, description = "插入浮点型键值")
    public void test05InsertDoubleKV() throws SQLException {
        String map1_value4_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value4.txt";
        insertKVToTbl(tableName1,"", map1_value4_path);
        String[][] dataArray = {
                {"7", "34.56", "{avg_price=34.89, total_sale=23575.47}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "id,amount,user_info";
        String queryState = " where id = 7";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 6, enabled = true, description = "插入整数和浮点数混合键值")
    public void test06InsertIntAndDoubleKV() throws SQLException {
        String map1_value5_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value5.txt";
        insertKVToTbl(tableName1,"", map1_value5_path);
        String[][] dataArray = {
                {"8", "zhangsan", "20", "34.56", "{avg_price=34.89, good_cnt=10011, total_sale=23575.47}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 8";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 7, enabled = true, dataProvider = "mapFieldMethod", expectedExceptions = SQLException.class, description = "插入字符型和数值型混合键值，预期插入失败")
    public void test07InsertVarcharAndNumKV(Map<String, String> param) throws SQLException {
        mapObj.insertKV(param.get("tableName"),"", param.get("mapValues"));
    }

    @Test(priority = 8, enabled = true, description = "插入布尔型键值")
    public void test08InsertBooleanKV() throws SQLException {
        String map1_value6_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value6.txt";
        insertKVToTbl(tableName1,"", map1_value6_path);
        String[][] dataArray = {
                {"34.56", "{in_use=true, is_delete=false}", "zhangsan", "9"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "amount,user_info,name,id";
        String queryState = " where id = 9";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 9, enabled = true, description = "插入列值为Null，预期可插入成功")
    public void test09InsertColumnNull() throws SQLException {
        String map1_value7_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value7.txt";
        insertKVToTbl(tableName1,"", map1_value7_path);
        String[][] dataArray = {
                {"10", "zhangsan", "20", "34.56", null}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 10";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 10, enabled = true, description = "删除列值为null的行数据，预期可删除成功")
    public void test10DeleteRowWithNull() throws SQLException {
        String queryFields = "*";
        String queryState = " where id = 10";
        int actualDelete = mapObj.deleteTableData(tableName1, queryState);
        System.out.println("Actual delete " + actualDelete + " rows");
        Assert.assertEquals(actualDelete, 1);

        Boolean actualResult = mapObj.queryResultSet(tableName1, queryFields, queryState);
        System.out.println("Actual: " + actualResult);
        Assert.assertFalse(actualResult);
    }

    @Test(priority = 11, enabled = true, description = "插入键值为空字符串")
    public void test11InsertBlankKV() throws SQLException {
        String map1_value8_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value8.txt";
        insertKVToTbl(tableName1,"", map1_value8_path);
        String[][] dataArray = {
                {"11", "LaLa", "27", "132.5", "{=class1, birthday=, sex=}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 11";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 12, enabled = true, description = "插入重复键，只保留最后一个")
    public void test12InsertDuplicateK() throws SQLException {
        String map1_value9_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value9.txt";
        insertKVToTbl(tableName1,"", map1_value9_path);
        String[][] dataArray = {
                {"12", "LaLa", "27", "132.5", "{birthday=2010}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 12";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 13, enabled = true, description = "允许键不同，值相同")
    public void test13InsertDuplicateV() throws SQLException {
        String map1_value10_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value10.txt";
        insertKVToTbl(tableName1,"", map1_value10_path);
        String[][] dataArray = {
                {"13", "LaLa", "27", "132.5", "{birthday1=2022, birthday2=2022}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 13";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 14, enabled = true, description = "指定map字段插入")
    public void test14InsertWithFieldsSpecified() throws SQLException {
        String map1_value11_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value11.txt";
        String insertFields = "(id,user_info)";
        insertKVToTbl(tableName1, insertFields, map1_value11_path);
        String[][] dataArray = {
                {"14", null, null, null, "{birthday=1999-12-19, height=185.5, sex=male}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 14";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 14, enabled = true, description = "map字段允许为Null, 不指定map字段插入")
    public void test14InsertWithoutFieldsSpecified() throws SQLException {
        String map1_value12_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl1_value12.txt";
        String insertFields = "(id,name,age,amount)";
        insertKVToTbl(tableName1, insertFields, map1_value12_path);
        String[][] dataArray = {
                {"15", "zhangsan", "55", "22.33", null}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 15";
        List<List> actualList = mapObj.queryTableData(tableName1, queryFields, queryState,5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

//    @Test(priority = 15, enabled = true, description = "创建含有多个Map类型字段的数据表，不指定默认值")
//    public void test15Table2CreateWithMultiMapFields() throws SQLException {
//        String map_meta2_path = "src/test/resources/tabledata/meta/complexdatatype/map/map_tbl2_meta.txt";
//        initMapTable(tableName2, map_meta2_path);
//    }

    @Test(priority = 16, enabled = true, description = "向含有多个Map类型字段的表中插入数据")
    public void test16InsertValuesToMultiMapFields() throws SQLException {
        String map2_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl2_value1.txt";
        insertKVToTbl(tableName2,"", map2_value1_path);
        String[][] dataArray = {
                {"1", "LaLa", "27", "132.5", "{birthday1=1998-12-11, birthday2=2010-09-15, sex=female}", "{score=98, sno=3001, tno=1476821}", "{avg_sale=1234.5678, price=23.58}"},
                {"2", "zhangsan", "39", "100.0", "{birthday1=1949-07-26, birthday2=2013-03-13, sex=female}", "{score=90, sno=3002, tno=2290832}", "{avg_sale=10000.0, price=67.33}"},
                {"3", "Huha", "56", "-78.6", "{birthday1=1970-01-01, birthday2=2000-10-10, sex=male}", "{score=78, sno=3003, tno=1809000}", "{avg_sale=39287.23, price=111.5}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id in (1,2,3)";
        List<List> actualList = mapObj.queryTableData(tableName2, queryFields, queryState, 7);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 17, enabled = true, description = "验证整型范围, 支持范围")
    public void test17VerifyIntValueRangeSupport() throws SQLException {
        String map2_value2_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl2_value2.txt";
        insertKVToTbl(tableName2,"", map2_value2_path);
        String[][] dataArray = {
                {"4", "kili", "17", "13.22", "{birthday1=2147483648, birthday2=-9223372036854775808}", "{score=2147483649, sno=13897654321, tno=9223372036854775807}", "{avg_sale=-2147483648, price=2147483647}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = " where id = 4";
        List<List> actualList = mapObj.queryTableData(tableName2, queryFields, queryState, 7);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }


    @Test(priority = 17, enabled = true, dataProvider = "mapFieldMethod", expectedExceptions = SQLException.class, description = "验证整型范围, 不支持范围")
    public void test17VerifyIntValueRangeNotSupport(Map<String, String> param) throws SQLException {
        insertKVToTbl(param.get("tableName"),"", param.get("valueFilePath"));
    }

    @Test(priority = 18, enabled = true, description = "删除数据")
    public void test18DeleteMapRow() throws SQLException {
        String deleteQuery = " where id = 2";
        int deleteRows = mapObj.deleteTableData(tableName2, deleteQuery);
        Assert.assertEquals(deleteRows, 1);

        String[][] dataArray = {
                {"1", "LaLa", "27", "132.5", "{birthday1=1998-12-11, birthday2=2010-09-15, sex=female}", "{score=98, sno=3001, tno=1476821}", "{avg_sale=1234.5678, price=23.58}"},
                {"3", "Huha", "56", "-78.6", "{birthday1=1970-01-01, birthday2=2000-10-10, sex=male}", "{score=78, sno=3003, tno=1809000}", "{avg_sale=39287.23, price=111.5}"},
                {"4", "kili", "17", "13.22", "{birthday1=2147483648, birthday2=-9223372036854775808}", "{score=2147483649, sno=13897654321, tno=9223372036854775807}", "{avg_sale=-2147483648, price=2147483647}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected after delete: " + expectedList);
        String queryFields = "*";
        String queryState = " where id <= 4";
        List<List> actualList = mapObj.queryTableData(tableName2, queryFields, queryState, 7);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

//    @Test(priority = 19, enabled = true, description = "创建含有Map类型字段的数据表，指定默认值")
//    public void test19Table4CreateMapHaveDefault() throws SQLException {
//        String map_meta4_path = "src/test/resources/tabledata/meta/complexdatatype/map/map_tbl4_meta.txt";
//        initMapTable(tableName4, map_meta4_path);
//    }

    @Test(priority = 20, enabled = true, description = "向有Map默认值的表插入数据，指定字段不含Map类型字段")
    public void test20InsertWithMapDefault() throws SQLException {
        String map4_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl4_value1.txt";
        insertKVToTbl(tableName4,"(id,name,age,amount)", map4_value1_path);
        String[][] dataArray = {
                {"1", "LaLa", "27", "132.5", "{birthday=2022-01-01, sex=male}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = "";
        List<List> actualList = mapObj.queryTableData(tableName4, queryFields, queryState, 5);
        System.out.println("Actual: " + actualList);
        Assert.assertEquals(actualList, expectedList);
    }

//    @Test(priority = 21, enabled = true, description = "Map类型字段在中间列")
//    public void test21TableCreateMapInMid() throws SQLException {
//        String map_meta5_path = "src/test/resources/tabledata/meta/complexdatatype/map/map_tbl5_meta.txt";
//        initMapTable(tableName5, map_meta5_path);
//    }

    @Test(priority = 22, enabled = true, description = "Map在中间列插入数据")
    public void test22InsertWithMapInMid() throws SQLException {
        String map5_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl5_value1.txt";
        insertKVToTbl(tableName5,"", map5_value1_path);
        String[][] dataArray = {
                {"1", "zhangsan", "{address=beijing, class_no=1024}", "20", "34.56"},
                {"2", "wangwu", "{address=dalian, sex=male}", "8", "0.0"},
                {"3", "liuyi", "{address=wuhan, class_no=1001, sex=female}", "-56", "-1834.29"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = "";
        List<List> actualList = mapObj.queryTableData(tableName5, queryFields, queryState, 5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

//    @Test(priority = 23, enabled = true, description = "Map类型字段在首列")
//    public void test23TableCreateMapAtFirst() throws SQLException {
//        String map_meta6_path = "src/test/resources/tabledata/meta/complexdatatype/map/map_tbl6_meta.txt";
//        initMapTable(tableName6, map_meta6_path);
//    }

    @Test(priority = 24, enabled = true, description = "Map在首列插入数据")
    public void test24InsertWithMapAtFirst() throws SQLException {
        String map6_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl6_value1.txt";
        insertKVToTbl(tableName6,"", map6_value1_path);
        String[][] dataArray = {
                {"{address=beijing, class_no=1024}","1998-04-06","08:10:10","2022-04-08 18:05:07","true","zhangsan","20","34.56","5001"},
                {"{address=dalian, sex=male}","1949-01-01","00:30:08","2022-12-01 01:02:03","false","wangwu","8","0.0","5002"},
                {"{address=wuhan, class_no=1001, sex=female}","2022-03-04","07:03:15","1999-02-28 23:59:59","false","liuyi","-56","-1834.29","5003"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "*";
        String queryState = "";
        List<List> actualList = mapObj.queryTableData(tableName6, queryFields, queryState, 9);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 25, enabled = true, expectedExceptions = SQLException.class,
            description = "创建表，map类型不允许为null，插入数据不指定map字段，预期失败")
    public void test25Table3CreateMapNotNull() throws SQLException {
//        String map_meta3_path = "src/test/resources/tabledata/meta/complexdatatype/map/map_tbl3_meta.txt";
//        initMapTable(tableName3, map_meta3_path);
        String map3_value1_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl3_value1.txt";
        String insertFields = "(id,name,age,amount)";
        insertKVToTbl(tableName3, insertFields, map3_value1_path);
    }

    @Test(priority = 26, enabled = true, description = "范围查询1")
    public void test26QueryByRange1() throws SQLException {
        String map3_value2_path = "src/test/resources/tabledata/value/complexdatatype/map/map_tbl3_value2.txt";
        insertKVToTbl(tableName3,"", map3_value2_path);
        String[][] dataArray = {
                {"-1230.44", "{address=shanghai, birthday=1962-06-20, sex=male}"},
                {"99.9", "{address=wuhan, birthday=1987-02-28, sex=female}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "amount,user_info";
        String queryState = " where id>1 and age<30";
        List<List> actualList = mapObj.queryTableData(tableName3, queryFields, queryState, 5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 26, enabled = true, description = "范围查询2")
    public void test26QueryByRange2() throws SQLException {
        String[][] dataArray = {
                {"1", "zhangsan", "{address=beijing, birthday=2022-01-10, sex=male}"},
                {"2", "lisi", "{address=beijing, birthday=2000-11-23, sex=female}"},
                {"3", "wangwu", "{address=shanghai, birthday=1962-06-20, sex=male}"}
        };
        List<List> expectedList = expectedMapOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "id,name,user_info";
        String queryState = " where id<3 or amount<0";
        List<List> actualList = mapObj.queryTableData(tableName3, queryFields, queryState, 5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    // ***********************************************************************************************
    // v0.5.0期间补充用例

    @Test(priority = 27, enabled = true, dataProvider = "mapFieldMethod", description = "不同类型key值，插入成功用例")
    public void test27DifferentKeyTypePos(Map<String, String> param) throws SQLException {
        String map_value_path = param.get("valuePath");
        insertKVToTbl(param.get("tableName"), param.get("insertFields"), map_value_path);

        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"),";","&");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = mapObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryState"),5);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 28, enabled = true, dataProvider = "mapFieldMethod", description = "更新非map类型列值")
    public void test28UpdateCommonCol(Map<String, String> param) throws SQLException {
        String map_value_path = param.get("valuePath");
        insertKVToTbl(param.get("tableName"), param.get("insertFields"), map_value_path);

        int updateRows = mapObj.mapTableUpdateCommonCol(param.get("updateState"));
        Assert.assertEquals(updateRows, Integer.parseInt(param.get("updateRows")));

        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("outData"),";","&");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = mapObj.queryTableData(param.get("tableName"), param.get("queryFields"), param.get("queryState"),12);
        System.out.println("Actual: " + actualList);
        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();
        try{
            tearDownStatement = mapObj.connection.createStatement();
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
            JDBCUtils.closeResource(mapObj.connection, tearDownStatement);
        }
    }
}
