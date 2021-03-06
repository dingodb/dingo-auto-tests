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

import io.dingodb.dailytest.NumericFuncs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestNumericFuncs {
    public static NumericFuncs numericObj = new NumericFuncs();

    public void initNumtestTalbe() throws SQLException {
        String numtest_meta_path = "src/test/resources/testdata/tablemeta/numtest_meta.txt";
        String numtest_value_path = "src/test/resources/testdata/tableInsertValues/numtest_value.txt";
        String numtest_meta = FileReaderUtil.readFile(numtest_meta_path);
        String numtest_value = FileReaderUtil.readFile(numtest_value_path);
        numericObj.numericTableCreate(numtest_meta);
        numericObj.insertTableValues(numtest_value);
    }

    public List expectedPowList1() {
        List powList1 = new ArrayList();
        String[] dataArray = {"10","625","8000","810000","0",null,"64339296875","6975757441"};
        for (int i=0; i<dataArray.length; i++) {
            powList1.add(dataArray[i]);
        }
        return powList1;
    }

    public List expectedPowList2() {
        List powList2 = new ArrayList();
        String[] dataArray = {"2.58","11904.9921",null,"0","-9127173372.88918"};
        for (int i=0; i<dataArray.length; i++) {
            powList2.add(dataArray[i]);
        }
        return powList2;
    }

    public List expectedRoundList1() {
        List roundList1 = new ArrayList();
        String[] dataArray = {"2.6","109.1",null,"0.0","-98.2","-5332.0","56.0","-343.5"};
        for (int i=0; i<dataArray.length; i++) {
            roundList1.add(dataArray[i]);
        }
        return roundList1;
    }

    public List expectedRoundList2() {
        List roundList2 = new ArrayList();
        String[] dataArray = {"3","109",null,"0","-98","-5332","56","-343"};
        for (int i=0; i<dataArray.length; i++) {
            roundList2.add(dataArray[i]);
        }
        return roundList2;
    }

    public List expectedRoundList3() {
        List roundList3 = new ArrayList();
        String[] dataArray = {"100","-100","-5300","100","-300"};
        for (int i=0; i<dataArray.length; i++) {
            roundList3.add(dataArray[i]);
        }
        return roundList3;
    }

    public List expectedCeilingList1() {
        List ceilingList1 = new ArrayList();
        String[] dataArray = {"3","110",null,"0","-98","-5331","56","-343"};
        for (int i=0; i<dataArray.length; i++) {
            ceilingList1.add(dataArray[i]);
        }
        return ceilingList1;
    }

    public List expectedFloorList1() {
        List floorList1 = new ArrayList();
        String[] dataArray = {"2","109",null,"0","-99","-5332","56","-344"};
        for (int i=0; i<dataArray.length; i++) {
            floorList1.add(dataArray[i]);
        }
        return floorList1;
    }

    public List expectedABSList1() {
        List absList1 = new ArrayList();
        String[] dataArray = {"2.58","109.11",null,"0.0","98.19","5331.9843","56","343.45"};
        for (int i=0; i<dataArray.length; i++) {
            absList1.add(dataArray[i]);
        }
        return absList1;
    }

    public List expectedModList1() {
        List modList1 = new ArrayList();
        String[] dataArray = {"0","1","2","2","0",null,"0","1"};
        for (int i=0; i<dataArray.length; i++) {
            modList1.add(dataArray[i]);
        }
        return modList1;
    }

    public List expectedModList2() {
        List modList2 = new ArrayList();
        String[] dataArray = {"2.58","9.11",null,"0",null,null,"21","-3.45"};
        for (int i=0; i<dataArray.length; i++) {
            modList2.add(dataArray[i]);
        }
        return modList2;
    }

    public List expectedModList3() {
        List modList3 = new ArrayList();
        String[] dataArray = {"2.26","25",null,null,"0",null,"35","17"};
        for (int i=0; i<dataArray.length; i++) {
            modList3.add(dataArray[i]);
        }
        return modList3;
    }

    public List expectedModList4() {
        List modList4 = new ArrayList();
        String[] dataArray = {"2.26","25",null,"35"};
        for (int i=0; i<dataArray.length; i++) {
            modList4.add(dataArray[i]);
        }
        return modList4;
    }

    public List expectedModList5() {
        List modList5 = new ArrayList();
        Integer[] dataArray = {2,4,6,8};
        for (int i=0; i<dataArray.length; i++) {
            modList5.add(dataArray[i]);
        }
        return modList5;
    }

    public List expectedModList6() {
        List modList5 = new ArrayList();
        Integer[] dataArray = {1,3,5,7};
        for (int i=0; i<dataArray.length; i++) {
            modList5.add(dataArray[i]);
        }
        return modList5;
    }


    @BeforeClass(alwaysRun = true, description = "?????????????????????????????????")
    public static void setupAll() {
        Assert.assertNotNull(numericObj.connection);
    }

    @Test(enabled = true, description = "???????????????????????????")
    public void test00NumericFuncTableCreate() throws SQLException {
        initNumtestTalbe();
    }

    @Test(priority = 0, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????Pow?????????????????????")
    public void test01PowPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.powPositiveArg(param.get("num1"), param.get("num2"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 1, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????Pow????????????????????????????????????????????????")
    public void test02PowXStr(Map<String, String> param) throws SQLException {
        numericObj.powxStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 2, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????Pow?????????????????????????????????????????????")
    public void test03PowYDecimal(Map<String, String> param) throws SQLException {
        numericObj.powPositiveArg(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 3, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????Pow????????????????????????????????????????????????")
    public void test04PowYStr(Map<String, String> param) throws SQLException {
        numericObj.powyStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 4, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????Pow??????????????????????????????????????????")
    public void test05PowWrongArg(Map<String, String> param) throws SQLException {
        numericObj.powWrongArg(param.get("powState"));
    }

    @Test(priority = 5, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????round?????????????????????")
    public void test06RoundPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.roundPositiveArg(param.get("inputNum"), param.get("decimalLen"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 6, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????round?????????????????????????????????????????????")
    public void test07RoundYDecimal(Map<String, String> param) throws SQLException {
        numericObj.roundPositiveArg(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????round????????????????????????????????????????????????")
    public void test08RoundXStr(Map<String, String> param) throws SQLException {
        numericObj.roundxStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 8, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????round????????????????????????????????????????????????")
    public void test09RoundYStr(Map<String, String> param) throws SQLException {
        numericObj.roundyStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 9, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????round??????????????????????????????????????????")
    public void test10RoundWrongArg(Map<String, String> param) throws SQLException {
        numericObj.roundWrongArg(param.get("roundState"));
    }

    @Test(priority = 10, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????round????????????????????????????????????????????????")
    public void test11RoundXYStr(Map<String, String> param) throws SQLException {
        numericObj.roundxyStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 11, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????round??????????????????x??????????????????0???????????????")
    public void test12RoundXOnly(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.roundXOnly(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????ceiling??????????????????????????????x??????????????????")
    public void test13CeilingPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.ceilingPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 13, enabled = true, expectedExceptions = SQLException.class, description = "??????ceiling???????????????????????????????????????")
    public void test14CeilingWrongArg(Map<String, String> param) throws SQLException {
        numericObj.ceilingWrongArg(param.get("ceilingState"));
    }

    @Test(priority = 14, enabled = true, expectedExceptions = SQLException.class, description = "??????ceiling???????????????????????????????????????")
    public void test15CeilingStrArg(Map<String, String> param) throws SQLException {
        numericObj.ceilingStrArg(param.get("inputNum"));
    }

    @Test(priority = 15, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????ceil????????????ceiling??????")
    public void test16CeilFunc(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.ceilFunc(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 16, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????floor??????????????????????????????x??????????????????")
    public void test17FloorPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.floorPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 17, enabled = true, expectedExceptions = SQLException.class, description = "??????floor???????????????????????????????????????")
    public void test18FloorWrongArg(Map<String, String> param) throws SQLException {
        numericObj.floorWrongArg(param.get("floorState"));
    }

    @Test(priority = 18, enabled = true, expectedExceptions = SQLException.class, description = "??????floor???????????????????????????????????????")
    public void test19FloorStrArg(Map<String, String> param) throws SQLException {
        numericObj.floorStrArg(param.get("inputNum"));
    }

    @Test(priority = 19, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????floor????????????????????????????????????")
    public void test20ABSPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.absPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 20, enabled = true, expectedExceptions = SQLException.class, description = "??????abs???????????????????????????????????????")
    public void test21ABSWrongArg(Map<String, String> param) throws SQLException {
        numericObj.absWrongArg(param.get("absState"));
    }

    @Test(priority = 21, enabled = true, expectedExceptions = SQLException.class, description = "??????abs???????????????????????????????????????")
    public void test22ABSStrArg(Map<String, String> param) throws SQLException {
        numericObj.absStrArg(param.get("inputNum"));
    }

    @Test(priority = 22, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "??????mod?????????????????????")
    public void test23PowPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.modPositiveArg(param.get("num1"), param.get("num2"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 23, enabled = true, expectedExceptions = SQLException.class, description = "??????mod?????????????????????????????????")
    public void test24ModWrongArg(Map<String, String> param) throws SQLException {
        numericObj.modWrongArg(param.get("modState"));
    }

    @Test(priority = 24, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????mod????????????????????????????????????????????????")
    public void test25ModXStr(Map<String, String> param) throws SQLException {
        numericObj.modxStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 25, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????mod????????????????????????????????????????????????")
    public void test26ModYStr(Map<String, String> param) throws SQLException {
        numericObj.modyStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 26, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "??????mod????????????????????????????????????????????????")
    public void test27ModXYStr(Map<String, String> param) throws SQLException {
        numericObj.modxyStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 27, enabled = true, description = "??????pow????????????????????????")
    public void test28PowInTable1() throws SQLException {
        List expectedList = expectedPowList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.powInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 28, enabled = true, description = "??????pow????????????????????????")
    public void test29PowInTable2() throws SQLException {
        List expectedList = expectedPowList2();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.powInTable2();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 29, enabled = true, description = "??????round????????????????????????")
    public void test30RoundInTable1() throws SQLException {
        List expectedList = expectedRoundList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.roundInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 30, enabled = true, description = "??????round????????????????????????")
    public void test31RoundInTable2() throws SQLException {
        List expectedList = expectedRoundList2();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.roundInTable2();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 31, enabled = true, description = "??????round????????????????????????")
    public void test32RoundInTable3() throws SQLException {
        List expectedList = expectedRoundList3();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.roundInTable3();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 32, enabled = true, description = "??????ceiling????????????????????????")
    public void test33CeilingInTable1() throws SQLException {
        List expectedList = expectedCeilingList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.ceilingInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 33, enabled = true, description = "??????floor????????????????????????")
    public void test34FloorInTable1() throws SQLException {
        List expectedList = expectedFloorList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.floorInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 34, enabled = true, description = "??????abs????????????????????????")
    public void test35ABSInTable1() throws SQLException {
        List expectedList = expectedABSList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.absInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 35, enabled = true, description = "??????mod????????????????????????")
    public void test36ModInTable1() throws SQLException {
        List expectedList = expectedModList1();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInTable1();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 36, enabled = true, description = "??????mod????????????????????????")
    public void test37ModInTable2() throws SQLException {
        List expectedList = expectedModList2();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInTable2();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 37, enabled = true, description = "??????mod????????????????????????")
    public void test38ModInTable3() throws SQLException {
        List expectedList = expectedModList3();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInTable3();
        System.out.println("Actual: " + actualList);

        Assert.assertEquals(actualList, expectedList);
    }

    @Test(priority = 38, enabled = true, description = "??????mod????????????????????????")
    public void test39ModInTable4() throws SQLException {
        List expectedList = expectedModList4();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInTable4();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 39, enabled = true, description = "??????mod?????????where?????????????????????")
    public void test40ModInWhereState1() throws SQLException {
        List expectedList = expectedModList5();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInWhereState1();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 40, enabled = true, description = "??????mod?????????where?????????????????????")
    public void test41ModInWhereState2() throws SQLException {
        List expectedList = expectedModList6();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInWhereState2();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 41, enabled = true, description = "??????mod?????????where?????????????????????")
    public void test42ModInWhereState3() throws SQLException {
        List expectedList = expectedModList6();
        System.out.println("Expected: " + expectedList);
        List actualList = numericObj.modInWhereState3();
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @AfterClass(alwaysRun = true, description = "???????????????????????????????????????????????????")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try{
            tearDownStatement = numericObj.connection.createStatement();
            tearDownStatement.execute("delete from numtest");
            tearDownStatement.execute("drop table numtest");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if(tearDownStatement != null) {
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try{
                if(numericObj.connection != null) {
                    numericObj.connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
