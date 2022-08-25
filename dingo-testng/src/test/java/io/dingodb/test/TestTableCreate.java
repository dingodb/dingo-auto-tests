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
import io.dingodb.dailytest.TableCreate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestTableCreate extends YamlDataHelper {
    public static TableCreate tableCreateObj = new TableCreate();

    public void initTable1() throws SQLException {
        String table1_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable1_meta.txt";
        String table1_value_path = "src/test/resources/testdata/createTableTest/values/createtable1_values.txt";
        String table1_meta = FileReaderUtil.readFile(table1_meta_path);
        String table1_value = FileReaderUtil.readFile(table1_value_path);
        tableCreateObj.createTableWithDefaultValue(table1_meta);
        tableCreateObj.insertTable1Values(table1_value);
    }

    public void initTable2() throws SQLException {
        String table2_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable2_meta.txt";
        String table2_value_path = "src/test/resources/testdata/createTableTest/values/createtable2_values.txt";
        String table2_meta = FileReaderUtil.readFile(table2_meta_path);
        String table2_value = FileReaderUtil.readFile(table2_value_path);
        tableCreateObj.createTablePrimaryKeyVarchar(table2_meta);
        tableCreateObj.insertTable2Values(table2_value);
    }

    public void initTable3() throws SQLException {
        String table3_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable3_meta.txt";
        String table3_value_path = "src/test/resources/testdata/createTableTest/values/createtable3_values.txt";
        String table3_meta = FileReaderUtil.readFile(table3_meta_path);
        String table3_value = FileReaderUtil.readFile(table3_value_path);
        tableCreateObj.createTablePrimaryKeyDouble(table3_meta);
        tableCreateObj.insertTable3Values(table3_value);
    }

    public void initTable4() throws SQLException {
        String table4_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable4_meta.txt";
        String table4_value_path = "src/test/resources/testdata/createTableTest/values/createtable4_values.txt";
        String table4_meta = FileReaderUtil.readFile(table4_meta_path);
        String table4_value = FileReaderUtil.readFile(table4_value_path);
        tableCreateObj.createTablePrimaryKeyDate(table4_meta);
        tableCreateObj.insertTable4Values(table4_value);
    }

    public void initTable5() throws SQLException {
        String table5_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable5_meta.txt";
        String table5_value_path = "src/test/resources/testdata/createTableTest/values/createtable5_values.txt";
        String table5_meta = FileReaderUtil.readFile(table5_meta_path);
        String table5_value = FileReaderUtil.readFile(table5_value_path);
        tableCreateObj.createTablePrimaryKeyTime(table5_meta);
        tableCreateObj.insertTable5Values(table5_value);
    }

    public void initTable6() throws SQLException {
        String table6_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable6_meta.txt";
        String table6_value_path = "src/test/resources/testdata/createTableTest/values/createtable6_values.txt";
        String table6_meta = FileReaderUtil.readFile(table6_meta_path);
        String table6_value = FileReaderUtil.readFile(table6_value_path);
        tableCreateObj.createTablePrimaryKeyTimestamp(table6_meta);
        tableCreateObj.insertTable6Values(table6_value);
    }

    public void initTable7() throws SQLException {
        String table7_meta_path = "src/test/resources/testdata/createTableTest/meta/createtable7_meta.txt";
        String table7_value_path = "src/test/resources/testdata/createTableTest/values/createtable7_values.txt";
        String table7_meta = FileReaderUtil.readFile(table7_meta_path);
        String table7_value = FileReaderUtil.readFile(table7_value_path);
        tableCreateObj.createTablePrimaryKeyBoolean(table7_meta);
        tableCreateObj.insertTable7Values(table7_value);
    }


    //表1预期数据
    public static List<List> expectedTable1List() {
        String[][] dataArray = {
                {"1","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"},
                {"2","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表2预期数据
    public static List<List> expectedTable2List() {
        String[][] dataArray = {
                {"1","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","true"},
                {"1","lisi","25","shanghai","895.0","1988-02-05","06:15:08","2000-02-29 00:00:00","false"},
                {"1","lisi2","55","beijing","123.123","2022-03-04","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表3预期数据
    public static List<List> expectedTable3List() {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-04","07:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.123","2022-03-04","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表4预期数据
    public static List<List> expectedTable4List() {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-04","07:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表5预期数据
    public static List<List> expectedTable5List() {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-05","17:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表6预期数据
    public static List<List> expectedTable6List() {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","2000-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    //表7预期数据
    public static List<List> expectedTable7List() {
        String[][] dataArray = {
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","false"},
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","true"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }


    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(TableCreate.connection);
    }

    @Test(enabled = true, description = "创建测试表1,所有字段均不允许为null，所有字段均有默认值，只插入主键")
    public void test01AssignDefaultValue() throws SQLException {
        initTable1();
    }
    @Test(enabled = true, dependsOnMethods = {"test01AssignDefaultValue"}, description = "验证table1表数据,验证默认值")
    public void test01CheckDefaultValue() throws SQLException {
        List<List> expectedRecords = expectedTable1List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable1Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertEquals(actualRecords, expectedRecords);
    }

    @Test(enabled = true, description = "创建测试表2,varchar类型字段为主键,插入数据有相同主键")
    public void test02CreateTable2PrimaryKeyVarchar() throws SQLException {
        initTable2();
    }

    @Test(enabled = true, dependsOnMethods = {"test02CreateTable2PrimaryKeyVarchar"}, description = "验证table2表数据")
    public void test02Table2Records() throws SQLException {
        List<List> expectedRecords = expectedTable2List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable2Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表3,double类型字段为主键,插入数据有相同主键")
    public void test03CreateTable3PrimaryKeyDouble() throws SQLException {
        initTable3();
    }

    @Test(enabled = true, dependsOnMethods = {"test03CreateTable3PrimaryKeyDouble"}, description = "验证table3表数据")
    public void test03Table3Records() throws SQLException {
        List<List> expectedRecords = expectedTable3List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable3Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表4,date类型字段为主键,插入数据有相同主键")
    public void test04CreateTable4PrimaryKeyDate() throws SQLException {
        initTable4();
    }

    @Test(enabled = true, dependsOnMethods = {"test04CreateTable4PrimaryKeyDate"}, description = "验证table4表数据")
    public void test04Table4Records() throws SQLException {
        List<List> expectedRecords = expectedTable4List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable4Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表5,time类型字段为主键,插入数据有相同主键")
    public void test05CreateTable5PrimaryKeyTime() throws SQLException {
        initTable5();
    }

    @Test(enabled = true, dependsOnMethods = {"test05CreateTable5PrimaryKeyTime"}, description = "验证table5表数据")
    public void test05Table5Records() throws SQLException {
        List<List> expectedRecords = expectedTable5List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable5Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表6,timestamp类型字段为主键,插入数据有相同主键")
    public void test06CreateTable6PrimaryKeyTimestamp() throws SQLException {
        initTable6();
    }

    @Test(enabled = true, dependsOnMethods = {"test06CreateTable6PrimaryKeyTimestamp"}, description = "验证table6表数据")
    public void test06Table6Records() throws SQLException {
        List<List> expectedRecords = expectedTable6List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable6Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表7,布尔类型字段为主键,插入数据有相同主键")
    public void test07CreateTable7PrimaryKeyBoolean() throws SQLException {
        initTable7();
    }

    @Test(enabled = true, dependsOnMethods = {"test07CreateTable7PrimaryKeyBoolean"}, description = "验证table7表数据")
    public void test07Table7Records() throws SQLException {
        List<List> expectedRecords = expectedTable7List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable7Data();
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, dataProvider = "createTableMethod", description = "验证多主键表的创建")
    public void test08TableCreateWithMultiPrimaryKey(Map<String, String> param) throws SQLException {
        tableCreateObj.createTableWithMultiPrimaryKey(param.get("createState"));
    }

    @Test(enabled = true, dataProvider = "createTableMethod", dependsOnMethods = {"test08TableCreateWithMultiPrimaryKey"}, description = "验证多主键表的插入")
    public void test09InsertValuesWithMultiPrimaryKey(Map<String, String> param) throws SQLException {
        int expectedEffect = Integer.parseInt(param.get("effectRows"));
        System.out.println("Expected effect rows: " + expectedEffect);
        int actualEffect = tableCreateObj.insertTable1WithMultiPrimaryKey(param.get("insertValue"));
        System.out.println("Actual effect rows: " + actualEffect);
        Assert.assertEquals(actualEffect, expectedEffect);
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try{
            tearDownStatement = tableCreateObj.connection.createStatement();
            tearDownStatement.execute("delete from ctest001");
            tearDownStatement.execute("drop table ctest001");
            tearDownStatement.execute("delete from ctest002");
            tearDownStatement.execute("drop table ctest002");
            tearDownStatement.execute("delete from ctest003");
            tearDownStatement.execute("drop table ctest003");
            tearDownStatement.execute("delete from ctest004");
            tearDownStatement.execute("drop table ctest004");
            tearDownStatement.execute("delete from ctest005");
            tearDownStatement.execute("drop table ctest005");
            tearDownStatement.execute("delete from ctest006");
            tearDownStatement.execute("drop table ctest006");
            tearDownStatement.execute("delete from ctest007");
            tearDownStatement.execute("drop table ctest007");
            tearDownStatement.execute("delete from mpkey_tbl1");
            tearDownStatement.execute("drop table mpkey_tbl1");
            tearDownStatement.execute("delete from mpkey_tbl2");
            tearDownStatement.execute("drop table mpkey_tbl2");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(tableCreateObj.connection, tearDownStatement);
        }
    }

}
