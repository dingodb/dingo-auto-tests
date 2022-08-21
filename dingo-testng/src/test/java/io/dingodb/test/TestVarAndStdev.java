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
import io.dingodb.dailytest.AggregateFuncVARandSTDEV;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestVarAndStdev {

    public static AggregateFuncVARandSTDEV varstdevObj = new AggregateFuncVARandSTDEV();

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库，创建表格和插入数据")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(varstdevObj.connection);
    }

    @Test(priority = 0, enabled = true, description = "验证输出age字段样本方差值正确")
    public void test01VarCal() throws SQLException {
        String varTable1path = "src/test/resources/testdata/tableInsertValues/vartable1.txt";
        String varTable1Values = FileReaderUtil.readFile(varTable1path);
        varstdevObj.varTable1Create();
        varstdevObj.varTable1Insert(varTable1Values);
        Double expectedAgeVarCal1 = 22207.333333333332;
        System.out.println("Expected: " + expectedAgeVarCal1);
        Double actualAgeVarCal1 = varstdevObj.varTable1Age();
        System.out.println("Actual: " + actualAgeVarCal1);

        Assert.assertEquals(actualAgeVarCal1, expectedAgeVarCal1);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01VarCal"}, description = "验证输出amount字段样本方差值正确")
    public void test02VarDoubleCal() throws SQLException {
        Double expectedAmountVarCal1 = 569571.6864432128;
        System.out.println("Expected: " + expectedAmountVarCal1);
        Double actualAmountVarCal1 = varstdevObj.varTable1Amount();
        System.out.println("Actual: " + actualAmountVarCal1);

        Assert.assertEquals(actualAmountVarCal1, expectedAmountVarCal1);
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01VarCal"}, expectedExceptions = SQLException.class,
            description = "验证不支持varchar类型字段计算方差")
    public void test03VarvarcharCal() throws SQLException {
        try(Statement statementVarChar = varstdevObj.connection.createStatement()) {
            String varcharCalSQL = "select var(name) from vartest1";
            statementVarChar.executeQuery(varcharCalSQL);
        }
    }

    @Test(priority = 3, enabled = true, expectedExceptions = SQLException.class, description = "验证不支持日期类型字段计算方差")
    public void test04VarDateCal() throws SQLException {
        String varTable5path = "src/test/resources/testdata/tableInsertValues/vartable5.txt";
        String varTable5Values = FileReaderUtil.readFile(varTable5path);
        varstdevObj.varTable5Create();
        varstdevObj.varTable5Insert(varTable5Values);
        try(Statement statementDate = varstdevObj.connection.createStatement()) {
            String dateCalSQL = "select var(birthday) from vartest5";

            statementDate.executeQuery(dateCalSQL);
        }
    }

    @Test(priority = 4, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证不支持时间类型字段计算方差")
    public void test05VarTimeCal() throws SQLException {
        try(Statement statementTime = varstdevObj.connection.createStatement()) {
            String timeCalSQL = "select var(create_time) from vartest5";
            statementTime.executeQuery(timeCalSQL);
        }
    }

    @Test(priority = 5, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证不支持时间日期类型字段计算方差")
    public void test06VarDateTimeCal() throws SQLException {
        try(Statement statementDateTime = varstdevObj.connection.createStatement()) {
            String dateTimeCalSQL = "select var(update_time) from vartest5";
            statementDateTime.executeQuery(dateTimeCalSQL);
        }
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test01VarCal"}, description = "验证只一条数据计算返回null")
    public void test07VarOneRowCal() throws SQLException {
        Double actualAgeVarOneRowCal = varstdevObj.varTable1AgeOneRowCal();
        Assert.assertNull(actualAgeVarOneRowCal);
    }

    @Test(priority = 7, enabled = true, description = "验证统计样本方差跳过Null值")
    public void test08VarCalSkipNull() throws SQLException {
        String varTable2path = "src/test/resources/testdata/tableInsertValues/vartable2.txt";
        String varTable2Values = FileReaderUtil.readFile(varTable2path);
        varstdevObj.varTable2Create();
        varstdevObj.varTable2Insert(varTable2Values);
        Double expectedAgeVarCal2 = 26049.963636363642;
        System.out.println("Expected: " + expectedAgeVarCal2);
        Double actualAgeVarCal2 = varstdevObj.varTable2SkipNullAge();
        System.out.println("Actual: " + actualAgeVarCal2);

        Assert.assertEquals(actualAgeVarCal2, expectedAgeVarCal2);
    }

    @Test(priority = 8, enabled = true, description = "验证字段值均相同，样本方差为0")
    public void test09VarColumnSameValue() throws SQLException {
        String varTable3path = "src/test/resources/testdata/tableInsertValues/vartable3.txt";
        String varTable3Values = FileReaderUtil.readFile(varTable3path);
        varstdevObj.varTable3Create();
        varstdevObj.varTable3Insert(varTable3Values);
        Double actualVarColumnSameValue = varstdevObj.varTableColumnSameValues();
        System.out.println("Actual: " + actualVarColumnSameValue);

        Assert.assertEquals(actualVarColumnSameValue, Double.valueOf(0));
    }

    @Test(priority = 9, enabled = true, description = "验证字段值均为0，样本方差为0")
    public void test10VarColumnZero() throws SQLException {
        String varTable4path = "src/test/resources/testdata/tableInsertValues/vartable4.txt";
        String varTable4Values = FileReaderUtil.readFile(varTable4path);
        varstdevObj.varTable4Create();
        varstdevObj.varTable4Insert(varTable4Values);
        Double actualVarColumnZero = varstdevObj.varTableColumnZero();
        System.out.println("Actual: " + actualVarColumnZero);

        Assert.assertEquals(actualVarColumnZero, Double.valueOf(0));
    }

    @Test(priority = 10, enabled = true, dependsOnMethods = {"test04VarDateCal"}, description = "多个字段计算样本统计方差")
    public void test11VarMultiCol() throws SQLException {
        List expectedMultiColVarList = new ArrayList();
        expectedMultiColVarList.add(4.666666666666667);
        expectedMultiColVarList.add(1208.952380952381);
        expectedMultiColVarList.add(338846.0604531898);

        System.out.println("Expected: " + expectedMultiColVarList);
        List<Double> actualMultiColVarList = varstdevObj.multiColumnVar();
        System.out.println("Actual: " + actualMultiColVarList);

        Assert.assertEquals(actualMultiColVarList, expectedMultiColVarList);
    }

    @Test(priority = 11, enabled = true, description = "空表计算，返回null")
    public void test12VarEmptyTable() throws SQLException {
        varstdevObj.varTable6Create();
        Double actualEmptyTableVar = varstdevObj.varEmptyTableCal();

        Assert.assertNull(actualEmptyTableVar);
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test04VarDateCal"},description = "验证VAR和其他聚合函数一起使用")
    public void test13VarWithOtherAggrFunc() throws SQLException {
        List expectedVarAggrFuncList = new ArrayList();
        expectedVarAggrFuncList.add(44.0000);
        expectedVarAggrFuncList.add(1497);
        expectedVarAggrFuncList.add(1579.4229);
        expectedVarAggrFuncList.add(1);
        expectedVarAggrFuncList.add("2022-03-04");
        expectedVarAggrFuncList.add(3);

        System.out.println("Expected: " + expectedVarAggrFuncList);
        List actualVarAggrFuncList = varstdevObj.varWithOtherAggrFunc();
        System.out.println("Actual: " + actualVarAggrFuncList);

        Assert.assertEquals(actualVarAggrFuncList, expectedVarAggrFuncList);
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test04VarDateCal"},description = "验证stdev计算age字段标准偏差")
    public void test14StdevCal() throws SQLException {
        Double expectedAgeStdevCal1 = 34.76999253598397;
        System.out.println("Expected: " + expectedAgeStdevCal1);
        Double actualStdevAgeCal = varstdevObj.stdevTable5Age();
        System.out.println("Actual: " + actualStdevAgeCal);

        Assert.assertEquals(actualStdevAgeCal, expectedAgeStdevCal1);
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test08VarCalSkipNull"}, description = "验证统计标准方差跳过Null值")
    public void test15StdevCalSkipNull() throws SQLException {
        Double expectedAgeStdevCal2 = 161.40001126506664;
        System.out.println("Expected: " + expectedAgeStdevCal2);
        Double actualAgeStdevCal2 = varstdevObj.stdevTable2SkipNullAge();
        System.out.println("Actual: " + actualAgeStdevCal2);

        Assert.assertEquals(actualAgeStdevCal2, expectedAgeStdevCal2);
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test04VarDateCal"}, description = "验证只一条数据计算返回null")
    public void test16StdevOneRowCal() throws SQLException {
        Double actualAgeStdevOneRowCal = varstdevObj.stdevTable5AgeOneRowCal();
        Assert.assertNull(actualAgeStdevOneRowCal);
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test09VarColumnSameValue"}, description = "验证字段值均相同，标准偏差为0")
    public void test17StdevColumnSameValue() throws SQLException {
        Double actualStdevColumnSameValue = varstdevObj.stdevTableColumnSameValues();
        System.out.println("Actual: " + actualStdevColumnSameValue);

        Assert.assertEquals(actualStdevColumnSameValue, Double.valueOf(0));
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test10VarColumnZero"}, description = "验证字段值均为0，标准偏差为0")
    public void test18StdevColumnZero() throws SQLException {
        Double actualStdevColumnZero = varstdevObj.stdevTableColumnZero();
        System.out.println("Actual: " + actualStdevColumnZero);

        Assert.assertEquals(actualStdevColumnZero, Double.valueOf(0));
    }

    @Test(priority = 18, enabled = true, dependsOnMethods = {"test04VarDateCal"}, description = "验证输出amount字段标准偏差值正确")
    public void test19StdevDoubleCal() throws SQLException {
        Double expectedAmountStdevCal5 = 582.1048534870584;
        System.out.println("Expected: " + expectedAmountStdevCal5);
        Double actualAmountStdevCal5 = varstdevObj.stdevTable5Amount();
        System.out.println("Actual: " + actualAmountStdevCal5);

        Assert.assertEquals(actualAmountStdevCal5, expectedAmountStdevCal5);
    }

    @Test(priority = 19, enabled = true, dependsOnMethods = {"test04VarDateCal"}, expectedExceptions = SQLException.class,
            description = "验证不支持varchar类型字段计算标准偏差")
    public void test20StdevVarcharCal() throws SQLException {
        try(Statement statementVarCharStdev = varstdevObj.connection.createStatement()) {
            String varcharStdevSQL = "select stdev(name) from vartest5";
            statementVarCharStdev.executeQuery(varcharStdevSQL);
        }
    }

    @Test(priority = 20, enabled = true, dependsOnMethods = {"test04VarDateCal"}, expectedExceptions = SQLException.class,
            description = "验证不支持日期类型字段计算标准偏差")
    public void test21StdevDateCal() throws SQLException {
        try(Statement statementStdevDate = varstdevObj.connection.createStatement()) {
            String dateStdevSQL = "select stdev(birthday) from vartest5";
            statementStdevDate.executeQuery(dateStdevSQL);
        }
    }

    @Test(priority = 21, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证不支持时间类型字段计算标准偏差")
    public void test22StdevTimeCal() throws SQLException {
        try(Statement statementStdevTime = varstdevObj.connection.createStatement()) {
            String timeStdevSQL = "select Stdev(create_time) from vartest5";
            statementStdevTime.executeQuery(timeStdevSQL);
        }
    }

    @Test(priority = 22, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证不支持时间日期类型字段计算标准偏差")
    public void test23StdevDateTimeCal() throws SQLException {
        try(Statement statementStdevDateTime = varstdevObj.connection.createStatement()) {
            String dateTimeStdevSQL = "select Stdev(update_time) from vartest5";
            statementStdevDateTime.executeQuery(dateTimeStdevSQL);
        }
    }

    @Test(priority = 23, enabled = true, dependsOnMethods = {"test04VarDateCal"}, description = "多个字段计算标准偏差")
    public void test24StdevMultiCol() throws SQLException {
        List expectedMultiColStdevList = new ArrayList();
        expectedMultiColStdevList.add(2.160246899469287);
        expectedMultiColStdevList.add(34.76999253598397);
        expectedMultiColStdevList.add(582.1048534870584);

        System.out.println("Expected: " + expectedMultiColStdevList);
        List<Double> actualMultiColStdevList = varstdevObj.multiColumnStdev();
        System.out.println("Actual: " + actualMultiColStdevList);

        Assert.assertEquals(actualMultiColStdevList, expectedMultiColStdevList);
    }

    @Test(priority = 24, enabled = true, dependsOnMethods = {"test12VarEmptyTable"}, description = "空表计算，返回null")
    public void test25StdevEmptyTable() throws SQLException {
        Double actualEmptyTableStdev = varstdevObj.stdevEmptyTableCal();
        Assert.assertNull(actualEmptyTableStdev);
    }

    @Test(priority = 25, enabled = true, dependsOnMethods = {"test04VarDateCal"},description = "验证STDEV和其他聚合函数一起使用")
    public void test26StdevWithOtherAggrFunc() throws SQLException {
        List expectedStdevAggrFuncList = new ArrayList();
        expectedStdevAggrFuncList.add(38.7500);
        expectedStdevAggrFuncList.add(20.139099615755747);
        expectedStdevAggrFuncList.add(1050.6992556);
        expectedStdevAggrFuncList.add("HAHA");
        expectedStdevAggrFuncList.add("1999-02-28 23:59:59");
        expectedStdevAggrFuncList.add(4);

        System.out.println("Expected: " + expectedStdevAggrFuncList);
        List actualStdevAggrFuncList = varstdevObj.stdevWithOtherAggrFunc();
        System.out.println("Actual: " + actualStdevAggrFuncList);

        Assert.assertEquals(actualStdevAggrFuncList, expectedStdevAggrFuncList);
    }

    @Test(priority = 26, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证空参返回错误信息")
    public void test27VarBlankParamError() throws SQLException {
        try(Statement statementVarBlankParam = varstdevObj.connection.createStatement()) {
            String varBlankParamSQL = "select var() from vartest5";
            statementVarBlankParam.executeQuery(varBlankParamSQL);
        }
    }

    @Test(priority = 27, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test04VarDateCal"},
            description = "验证空参返回错误信息")
    public void test28StdevBlankParamError() throws SQLException {
        try(Statement statementStdevBlankParam = varstdevObj.connection.createStatement()) {
            String stdevBlankParamSQL = "select stdev() from vartest5";
            statementStdevBlankParam.executeQuery(stdevBlankParamSQL);
        }
    }

    @Test(priority = 28, enabled = true, expectedExceptions = SQLException.class, description = "验证不支持布尔型字段计算方差")
    public void test29VarBooleanCal() throws SQLException {
        String varTable7path = "src/test/resources/testdata/tableInsertValues/vartable7.txt";
        String varTable7Values = FileReaderUtil.readFile(varTable7path);
        varstdevObj.varTable7Create();
        varstdevObj.varTable7Insert(varTable7Values);
        try(Statement statementDate = varstdevObj.connection.createStatement()) {
            String booleanVarCalSQL = "select var(is_delete) from vartest7";
            statementDate.executeQuery(booleanVarCalSQL);
        }
    }

    @Test(priority = 29, enabled = true, dependsOnMethods = {"test29VarBooleanCal"}, expectedExceptions = SQLException.class,
            description = "验证不支持布尔型字段计算标准偏差")
    public void test30StdevBooleanCal() throws SQLException {
        try(Statement statementDate = varstdevObj.connection.createStatement()) {
            String booleanStdevCalSQL = "select stdev(is_delete) from vartest7";
            statementDate.executeQuery(booleanStdevCalSQL);
        }
    }

    @Test(priority = 30, enabled = true, description = "字段值均为null，返回null")
    public void test31VarAllNull() throws SQLException {
        String varTable8path = "src/test/resources/testdata/tableInsertValues/vartable8.txt";
        String varTable8Values = FileReaderUtil.readFile(varTable8path);
        varstdevObj.varTable8Create();
        varstdevObj.varTable8Insert(varTable8Values);

        Double actualVarAllNullCal8 = varstdevObj.varTable8Age();

        Assert.assertNull(actualVarAllNullCal8);
    }

    @Test(priority = 31, enabled = true, dependsOnMethods = {"test31VarAllNull"}, description = "字段值均为null，返回null")
    public void test32StdevAllNull() throws SQLException {
        Double actualStdevAllNullCal8 = varstdevObj.stdevTable8Age();
        Assert.assertNull(actualStdevAllNullCal8);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement teardownStatement = null;
        try {
            teardownStatement = AggregateFuncVARandSTDEV.connection.createStatement();
            teardownStatement.execute("delete from vartest1");
            teardownStatement.execute("drop table vartest1");
            teardownStatement.execute("delete from vartest2");
            teardownStatement.execute("drop table vartest2");
            teardownStatement.execute("delete from vartest3");
            teardownStatement.execute("drop table vartest3");
            teardownStatement.execute("delete from vartest4");
            teardownStatement.execute("drop table vartest4");
            teardownStatement.execute("delete from vartest5");
            teardownStatement.execute("drop table vartest5");
            teardownStatement.execute("delete from vartest7");
            teardownStatement.execute("drop table vartest7");
            teardownStatement.execute("delete from vartest8");
            teardownStatement.execute("drop table vartest8");
            teardownStatement.execute("delete from product");
            teardownStatement.execute("drop table product");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.closeResource(AggregateFuncVARandSTDEV.connection, teardownStatement);
        }
    }
}
