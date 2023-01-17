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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestTableCreate extends YamlDataHelper {
    public static TableCreate tableCreateObj = new TableCreate();
    public static String tableName1 = "ctest001";
    public static String tableName2 = "ctest002";
    public static String tableName3 = "ctest003";
    public static String tableName4 = "ctest004";
    public static String tableName5 = "ctest005";
    public static String tableName6 = "ctest006";
    public static String tableName7 = "ctest007";
    public static String tableName8 = "risk_record8";

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        tableCreateObj.createTableWithMeta(tableName, tableMeta);
    }

    public void insertValues(String tableName, String insertField, String tableValuePath) throws SQLException {
        String tableValue = FileReaderUtil.readFile(tableValuePath);
        tableCreateObj.insertTableValues(tableName, insertField, tableValue);
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


    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(TableCreate.connection);
    }

    @Test(enabled = true, description = "创建测试表1,所有字段均不允许为null，所有字段均有默认值，只插入主键")
    public void test01AssignDefaultValue() throws SQLException {
        String insertFields = "(id)";
        String table1_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable1_meta.txt";
        String table1_value_path = "src/test/resources/tabledata/value/createTableTest/createtable1_values.txt";
        initTable(tableName1, table1_meta_path);
        insertValues(tableName1, insertFields, table1_value_path);
    }
    @Test(enabled = true, dependsOnMethods = {"test01AssignDefaultValue"}, description = "验证table1表数据,验证默认值")
    public void test01CheckDefaultValue() throws SQLException {
        String[][] dataArray = {
                {"1","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"},
                {"2","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName1,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertEquals(actualRecords, expectedRecords);
    }

    @Test(enabled = true, description = "创建测试表2,varchar类型字段为主键,插入数据有相同主键")
    public void test02CreateTable2PrimaryKeyVarchar() throws SQLException {
        String table2_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable2_meta.txt";
        String table2_value_path = "src/test/resources/tabledata/value/createTableTest/createtable2_values.txt";
        initTable(tableName2, table2_meta_path);
        insertValues(tableName2, "", table2_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test02CreateTable2PrimaryKeyVarchar"}, description = "验证table2表数据")
    public void test02Table2Records() throws SQLException {
        String[][] dataArray = {
                {"1","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","true"},
                {"1","lisi","25","shanghai","895.0","1988-02-05","06:15:08","2000-02-29 00:00:00","false"},
                {"1","lisi2","55","beijing","123.123","2022-03-04","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName2,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表3,double类型字段为主键,插入数据有相同主键")
    public void test03CreateTable3PrimaryKeyDouble() throws SQLException {
        String table3_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable3_meta.txt";
        String table3_value_path = "src/test/resources/tabledata/value/createTableTest/createtable3_values.txt";
        initTable(tableName3, table3_meta_path);
        insertValues(tableName3,"", table3_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test03CreateTable3PrimaryKeyDouble"}, description = "验证table3表数据")
    public void test03Table3Records() throws SQLException {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-04","07:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.123","2022-03-04","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName3,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表4,date类型字段为主键,插入数据有相同主键")
    public void test04CreateTable4PrimaryKeyDate() throws SQLException {
        String table4_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable4_meta.txt";
        String table4_value_path = "src/test/resources/tabledata/value/createTableTest/createtable4_values.txt";
        initTable(tableName4, table4_meta_path);
        insertValues(tableName4,"", table4_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test04CreateTable4PrimaryKeyDate"}, description = "验证table4表数据")
    public void test04Table4Records() throws SQLException {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-04","07:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);
        List<List> actualRecords = tableCreateObj.queryTableData(tableName4,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表5,time类型字段为主键,插入数据有相同主键")
    public void test05CreateTable5PrimaryKeyTime() throws SQLException {
        String table5_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable5_meta.txt";
        String table5_value_path = "src/test/resources/tabledata/value/createTableTest/createtable5_values.txt";
        initTable(tableName5, table5_meta_path);
        insertValues(tableName5,"", table5_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test05CreateTable5PrimaryKeyTime"}, description = "验证table5表数据")
    public void test05Table5Records() throws SQLException {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-05","17:03:15","1999-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName5,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表6,timestamp类型字段为主键,插入数据有相同主键")
    public void test06CreateTable6PrimaryKeyTimestamp() throws SQLException {
        String table6_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable6_meta.txt";
        String table6_value_path = "src/test/resources/tabledata/value/createTableTest/createtable6_values.txt";
        initTable(tableName6, table6_meta_path);
        insertValues(tableName6,"", table6_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test06CreateTable6PrimaryKeyTimestamp"}, description = "验证table6表数据")
    public void test06Table6Records() throws SQLException {
        String[][] dataArray = {
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","false"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","2000-02-28 23:59:59","true"},
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName6,"*","",9);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, description = "创建测试表7,布尔类型字段为主键,插入数据有相同主键")
    public void test07CreateTable7PrimaryKeyBoolean() throws SQLException {
        String table7_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable7_meta.txt";
        String table7_value_path = "src/test/resources/tabledata/value/createTableTest/createtable7_values.txt";
        initTable(tableName7, table7_meta_path);
        insertValues(tableName7,"", table7_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test07CreateTable7PrimaryKeyBoolean"}, description = "验证table7表数据")
    public void test07Table7Records() throws SQLException {
        String[][] dataArray = {
                {"1","lisi","55","beijing","123.124","2022-03-05","07:03:15","1999-02-28 23:59:59","false"},
                {"2","zhangsan","18","shanghai","23.5","1998-04-06","08:10:10","2022-04-08 18:05:07","true"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTableData(tableName7,"*","",9);
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

    @Test(enabled = true, description = "创建测试表8,多个varchar类型字段为主键")
    public void test10CreateTable8PrimaryKeyVarchar() throws SQLException {
        String table8_meta_path = "src/test/resources/tabledata/meta/createTableTest/createtable8_meta.txt";
        String table8_value_path = "src/test/resources/tabledata/value/createTableTest/createtable8_values.txt";
        initTable(tableName8, table8_meta_path);
        insertValues(tableName8, "", table8_value_path);
    }

    @Test(enabled = true, dependsOnMethods = {"test10CreateTable8PrimaryKeyVarchar"}, description = "验证table8表数据")
    public void test11Table8Records() throws SQLException {
        String[][] dataArray = {
                {"9GAZ01rR-Guem-d3zx-w0gu-fMhOorDXAZyn","15021093678","2"},
                {"IaEG012Y-UBA1-zCG3-zuYc-lzy9YtoX4h2B","15313467582","0"},
                {"jOHtV0qo-IiXa-WnG9-vSwC-sB5MUZNparHl","13898723516","0"},
                {"3Flr9LHz-BJNu-HpJk-nDBk-JoDyTgMY1wIA","13875423819","0"},
                {"uw8lhzI2-BZEl-poiz-GyaF-n2grtyVe1sUw","15963095421","1"},
                {"Ohlfy9j2-wz5D-uh1Z-8hkt-FyYSjTkCrPp3","13891208456","2"},
                {"jDkglHwp-Ri97-bX2I-xb8N-2IxLHBtJ0w31","15930576819","1"},
                {"p7Fnxs4c-oOm4-f3MW-pbFU-tgsRT6hPfBnO","15109632451","2"},
                {"NuyEpXxS-w8Tc-9voZ-syu7-rzbKxfoTJSn9","15378964201","2"},
                {"l5IgaUdX-M6VN-5dcF-iHuV-1fUdWeSosvTr","15143756192","0"},
                {"VpYR7eCr-1vIA-bR58-SMNT-JX2uliA64gKQ","13127469183","1"},
                {"tcjZ7nzV-G5as-fINl-Pheb-idvrAHWNREj4","15127819340","0"},
                {"wfceDP8m-hB4I-z1dF-IJMX-VyruUwKmjZv8","15090571843","2"},
                {"aAIMU4hY-KBbW-XeUK-LmUY-qHOxZyd5Mam3","15305379461","2"},
                {"g8RJAl2c-oJpn-1wUW-Y4pw-vqVONHPsFwmM","13976593104","0"},
                {"ztqRfs0M-aiKp-FmUW-wrgi-o2jhdN7ygbtC","13829137460","1"},
                {"QdXzPYOp-f81J-Ed8t-SkcI-t13UOpIXWgEe","15003614859","1"},
                {"VhMOLgPt-bWXs-crsB-ysdL-k85rReQnZFaq","13153692417","0"},
                {"1jbNLQuZ-UBeZ-7kcL-XME4-DYaeQVksA0zo","15316049823","2"},
                {"24oLAvZl-s9rE-Sqk2-7QZN-aN0Pu9kwEQJO","13984396251","0"},
                {"7uaeMKqU-ixQ7-xzi2-H6rc-HzMGnEtKw1le","15986213795","0"},
                {"fyHp49b0-Vojs-cy0j-roDH-h0AaI4TqKYx3","15082043651","1"},
                {"Ut5a2zTp-WQuo-XheC-Qnh0-KSqnoR2gzQZa","15986723409","2"},
                {"1mUV58ky-KD1u-MIBV-3C2X-x3c48Z0qpCNF","15981639027","2"},
                {"OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV","15917928356","2"},
                {"OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV","15917928355","2"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        String queryFields = "uuid,phone,status";
        String filterState = " where uuid in (" +
                "'9GAZ01rR-Guem-d3zx-w0gu-fMhOorDXAZyn'," +
                "'IaEG012Y-UBA1-zCG3-zuYc-lzy9YtoX4h2B'," +
                "'jOHtV0qo-IiXa-WnG9-vSwC-sB5MUZNparHl'," +
                "'3Flr9LHz-BJNu-HpJk-nDBk-JoDyTgMY1wIA'," +
                "'uw8lhzI2-BZEl-poiz-GyaF-n2grtyVe1sUw'," +
                "'Ohlfy9j2-wz5D-uh1Z-8hkt-FyYSjTkCrPp3'," +
                "'jDkglHwp-Ri97-bX2I-xb8N-2IxLHBtJ0w31'," +
                "'p7Fnxs4c-oOm4-f3MW-pbFU-tgsRT6hPfBnO'," +
                "'NuyEpXxS-w8Tc-9voZ-syu7-rzbKxfoTJSn9'," +
                "'l5IgaUdX-M6VN-5dcF-iHuV-1fUdWeSosvTr'," +
                "'VpYR7eCr-1vIA-bR58-SMNT-JX2uliA64gKQ'," +
                "'tcjZ7nzV-G5as-fINl-Pheb-idvrAHWNREj4'," +
                "'wfceDP8m-hB4I-z1dF-IJMX-VyruUwKmjZv8'," +
                "'aAIMU4hY-KBbW-XeUK-LmUY-qHOxZyd5Mam3'," +
                "'g8RJAl2c-oJpn-1wUW-Y4pw-vqVONHPsFwmM'," +
                "'ztqRfs0M-aiKp-FmUW-wrgi-o2jhdN7ygbtC'," +
                "'QdXzPYOp-f81J-Ed8t-SkcI-t13UOpIXWgEe'," +
                "'VhMOLgPt-bWXs-crsB-ysdL-k85rReQnZFaq'," +
                "'1jbNLQuZ-UBeZ-7kcL-XME4-DYaeQVksA0zo'," +
                "'24oLAvZl-s9rE-Sqk2-7QZN-aN0Pu9kwEQJO'," +
                "'7uaeMKqU-ixQ7-xzi2-H6rc-HzMGnEtKw1le'," +
                "'fyHp49b0-Vojs-cy0j-roDH-h0AaI4TqKYx3'," +
                "'Ut5a2zTp-WQuo-XheC-Qnh0-KSqnoR2gzQZa'," +
                "'1mUV58ky-KD1u-MIBV-3C2X-x3c48Z0qpCNF'," +
                "'OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV'" +
                ")";

        List<List> actualRecords = tableCreateObj.queryTableData(tableName8, queryFields, filterState,5);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }

    @Test(enabled = true, dependsOnMethods = {"test11Table8Records"}, description = "验证通过in范围过滤对表进行更新操作")
    public void test12Table8Update() throws SQLException {
        String filterState = " where uuid in (" +
                "'9GAZ01rR-Guem-d3zx-w0gu-fMhOorDXAZyn'," +
                "'IaEG012Y-UBA1-zCG3-zuYc-lzy9YtoX4h2B'," +
                "'jOHtV0qo-IiXa-WnG9-vSwC-sB5MUZNparHl'," +
                "'3Flr9LHz-BJNu-HpJk-nDBk-JoDyTgMY1wIA'," +
                "'uw8lhzI2-BZEl-poiz-GyaF-n2grtyVe1sUw'," +
                "'Ohlfy9j2-wz5D-uh1Z-8hkt-FyYSjTkCrPp3'," +
                "'jDkglHwp-Ri97-bX2I-xb8N-2IxLHBtJ0w31'," +
                "'p7Fnxs4c-oOm4-f3MW-pbFU-tgsRT6hPfBnO'," +
                "'NuyEpXxS-w8Tc-9voZ-syu7-rzbKxfoTJSn9'," +
                "'l5IgaUdX-M6VN-5dcF-iHuV-1fUdWeSosvTr'," +
                "'VpYR7eCr-1vIA-bR58-SMNT-JX2uliA64gKQ'," +
                "'tcjZ7nzV-G5as-fINl-Pheb-idvrAHWNREj4'," +
                "'wfceDP8m-hB4I-z1dF-IJMX-VyruUwKmjZv8'," +
                "'aAIMU4hY-KBbW-XeUK-LmUY-qHOxZyd5Mam3'," +
                "'g8RJAl2c-oJpn-1wUW-Y4pw-vqVONHPsFwmM'," +
                "'ztqRfs0M-aiKp-FmUW-wrgi-o2jhdN7ygbtC'," +
                "'QdXzPYOp-f81J-Ed8t-SkcI-t13UOpIXWgEe'," +
                "'VhMOLgPt-bWXs-crsB-ysdL-k85rReQnZFaq'," +
                "'1jbNLQuZ-UBeZ-7kcL-XME4-DYaeQVksA0zo'," +
                "'24oLAvZl-s9rE-Sqk2-7QZN-aN0Pu9kwEQJO'," +
                "'7uaeMKqU-ixQ7-xzi2-H6rc-HzMGnEtKw1le'," +
                "'fyHp49b0-Vojs-cy0j-roDH-h0AaI4TqKYx3'," +
                "'Ut5a2zTp-WQuo-XheC-Qnh0-KSqnoR2gzQZa'," +
                "'1mUV58ky-KD1u-MIBV-3C2X-x3c48Z0qpCNF'," +
                "'OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV'" +
                ")";
        String execSql = "update " + tableName8 + " set status = 0" + filterState;
        int actualRows = tableCreateObj.writeOpRows(tableName8, execSql);
        Assert.assertEquals(actualRows, 17);
    }

    @Test(enabled = true, dependsOnMethods = {"test12Table8Update"}, description = "验证table8表更新后数据")
    public void test13Table8RecordsAfterUpdate() throws SQLException {
        String[][] dataArray = {
                {"9GAZ01rR-Guem-d3zx-w0gu-fMhOorDXAZyn","15021093678","0"},
                {"IaEG012Y-UBA1-zCG3-zuYc-lzy9YtoX4h2B","15313467582","0"},
                {"jOHtV0qo-IiXa-WnG9-vSwC-sB5MUZNparHl","13898723516","0"},
                {"3Flr9LHz-BJNu-HpJk-nDBk-JoDyTgMY1wIA","13875423819","0"},
                {"uw8lhzI2-BZEl-poiz-GyaF-n2grtyVe1sUw","15963095421","0"},
                {"Ohlfy9j2-wz5D-uh1Z-8hkt-FyYSjTkCrPp3","13891208456","0"},
                {"jDkglHwp-Ri97-bX2I-xb8N-2IxLHBtJ0w31","15930576819","0"},
                {"p7Fnxs4c-oOm4-f3MW-pbFU-tgsRT6hPfBnO","15109632451","0"},
                {"NuyEpXxS-w8Tc-9voZ-syu7-rzbKxfoTJSn9","15378964201","0"},
                {"l5IgaUdX-M6VN-5dcF-iHuV-1fUdWeSosvTr","15143756192","0"},
                {"VpYR7eCr-1vIA-bR58-SMNT-JX2uliA64gKQ","13127469183","0"},
                {"tcjZ7nzV-G5as-fINl-Pheb-idvrAHWNREj4","15127819340","0"},
                {"wfceDP8m-hB4I-z1dF-IJMX-VyruUwKmjZv8","15090571843","0"},
                {"aAIMU4hY-KBbW-XeUK-LmUY-qHOxZyd5Mam3","15305379461","0"},
                {"g8RJAl2c-oJpn-1wUW-Y4pw-vqVONHPsFwmM","13976593104","0"},
                {"ztqRfs0M-aiKp-FmUW-wrgi-o2jhdN7ygbtC","13829137460","0"},
                {"QdXzPYOp-f81J-Ed8t-SkcI-t13UOpIXWgEe","15003614859","0"},
                {"VhMOLgPt-bWXs-crsB-ysdL-k85rReQnZFaq","13153692417","0"},
                {"1jbNLQuZ-UBeZ-7kcL-XME4-DYaeQVksA0zo","15316049823","0"},
                {"24oLAvZl-s9rE-Sqk2-7QZN-aN0Pu9kwEQJO","13984396251","0"},
                {"7uaeMKqU-ixQ7-xzi2-H6rc-HzMGnEtKw1le","15986213795","0"},
                {"fyHp49b0-Vojs-cy0j-roDH-h0AaI4TqKYx3","15082043651","0"},
                {"Ut5a2zTp-WQuo-XheC-Qnh0-KSqnoR2gzQZa","15986723409","0"},
                {"1mUV58ky-KD1u-MIBV-3C2X-x3c48Z0qpCNF","15981639027","0"},
                {"OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV","15917928356","0"},
                {"OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV","15917928355","0"}
        };
        List<List> expectedRecords = expectedOutData(dataArray);
        System.out.println("Expected: " + expectedRecords);

        String queryFields = "uuid,phone,status";
        String filterState = " where uuid in (" +
                "'9GAZ01rR-Guem-d3zx-w0gu-fMhOorDXAZyn'," +
                "'IaEG012Y-UBA1-zCG3-zuYc-lzy9YtoX4h2B'," +
                "'jOHtV0qo-IiXa-WnG9-vSwC-sB5MUZNparHl'," +
                "'3Flr9LHz-BJNu-HpJk-nDBk-JoDyTgMY1wIA'," +
                "'uw8lhzI2-BZEl-poiz-GyaF-n2grtyVe1sUw'," +
                "'Ohlfy9j2-wz5D-uh1Z-8hkt-FyYSjTkCrPp3'," +
                "'jDkglHwp-Ri97-bX2I-xb8N-2IxLHBtJ0w31'," +
                "'p7Fnxs4c-oOm4-f3MW-pbFU-tgsRT6hPfBnO'," +
                "'NuyEpXxS-w8Tc-9voZ-syu7-rzbKxfoTJSn9'," +
                "'l5IgaUdX-M6VN-5dcF-iHuV-1fUdWeSosvTr'," +
                "'VpYR7eCr-1vIA-bR58-SMNT-JX2uliA64gKQ'," +
                "'tcjZ7nzV-G5as-fINl-Pheb-idvrAHWNREj4'," +
                "'wfceDP8m-hB4I-z1dF-IJMX-VyruUwKmjZv8'," +
                "'aAIMU4hY-KBbW-XeUK-LmUY-qHOxZyd5Mam3'," +
                "'g8RJAl2c-oJpn-1wUW-Y4pw-vqVONHPsFwmM'," +
                "'ztqRfs0M-aiKp-FmUW-wrgi-o2jhdN7ygbtC'," +
                "'QdXzPYOp-f81J-Ed8t-SkcI-t13UOpIXWgEe'," +
                "'VhMOLgPt-bWXs-crsB-ysdL-k85rReQnZFaq'," +
                "'1jbNLQuZ-UBeZ-7kcL-XME4-DYaeQVksA0zo'," +
                "'24oLAvZl-s9rE-Sqk2-7QZN-aN0Pu9kwEQJO'," +
                "'7uaeMKqU-ixQ7-xzi2-H6rc-HzMGnEtKw1le'," +
                "'fyHp49b0-Vojs-cy0j-roDH-h0AaI4TqKYx3'," +
                "'Ut5a2zTp-WQuo-XheC-Qnh0-KSqnoR2gzQZa'," +
                "'1mUV58ky-KD1u-MIBV-3C2X-x3c48Z0qpCNF'," +
                "'OBHNtTS9-rAPz-G0Ns-03Ro-LJkNepB1IyqV'" +
                ")";

        List<List> actualRecords = tableCreateObj.queryTableData(tableName8, queryFields, filterState,5);
        System.out.println("Actual: " + actualRecords);
        Assert.assertTrue(actualRecords.containsAll(expectedRecords));
        Assert.assertTrue(expectedRecords.containsAll(actualRecords));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList("ctest001", "ctest002", "ctest003", "ctest004",
                "ctest005", "ctest006", "ctest007", "mpkey_tbl1", "mpkey_tbl2", "risk_record8"
                );
        try{
            tearDownStatement = tableCreateObj.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
//            tearDownStatement.execute("drop table ctest001");
//            tearDownStatement.execute("drop table ctest002");
//            tearDownStatement.execute("drop table ctest003");
//            tearDownStatement.execute("drop table ctest004");
//            tearDownStatement.execute("drop table ctest005");
//            tearDownStatement.execute("drop table ctest006");
//            tearDownStatement.execute("drop table ctest007");
//            tearDownStatement.execute("drop table mpkey_tbl1");
//            tearDownStatement.execute("drop table mpkey_tbl2");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(tableCreateObj.connection, tearDownStatement);
        }
    }

}
