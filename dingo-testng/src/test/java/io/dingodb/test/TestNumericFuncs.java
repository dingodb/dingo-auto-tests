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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Map;

public class TestNumericFuncs {
    public static NumericFuncs numericObj = new NumericFuncs();

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(numericObj.connection);
    }

    @Test(priority = 0, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证Pow函数，正向用例")
    public void test01PowPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.powPositiveArg(param.get("num1"), param.get("num2"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 1, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证Pow函数第一个参数为字符串，预期失败")
    public void test02PowXStr(Map<String, String> param) throws SQLException {
        numericObj.powxStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 2, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证Pow函数第二个参数为小数，预期失败")
    public void test03PowYDecimal(Map<String, String> param) throws SQLException {
        numericObj.powPositiveArg(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 3, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证Pow函数第二个参数为字符串，预期失败")
    public void test04PowYStr(Map<String, String> param) throws SQLException {
        numericObj.powyStr(param.get("num1"), param.get("num2"));
    }

    @Test(priority = 4, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证Pow函数输入参数不合法，预期失败")
    public void test05PowWrongArg(Map<String, String> param) throws SQLException {
        numericObj.powWrongArg(param.get("powState"));
    }

    @Test(priority = 5, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证round函数，正向用例")
    public void test06RoundPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.roundPositiveArg(param.get("inputNum"), param.get("decimalLen"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 6, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证round函数第二个参数为小数，预期失败")
    public void test07RoundYDecimal(Map<String, String> param) throws SQLException {
        numericObj.roundPositiveArg(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证round函数第一个参数为字符串，预期失败")
    public void test08RoundXStr(Map<String, String> param) throws SQLException {
        numericObj.roundxStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 8, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证round函数第二个参数为字符串，预期失败")
    public void test09RoundYStr(Map<String, String> param) throws SQLException {
        numericObj.roundyStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 9, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证round函数输入参数不合法，预期失败")
    public void test10RoundWrongArg(Map<String, String> param) throws SQLException {
        numericObj.roundWrongArg(param.get("roundState"));
    }

    @Test(priority = 10, enabled = true, dataProvider = "yamlNumericFuncMethod", expectedExceptions = SQLException.class,
            description = "验证round函数两个参数均为字符串，预期失败")
    public void test11RoundXYStr(Map<String, String> param) throws SQLException {
        numericObj.roundxyStr(param.get("inputNum"), param.get("decimalLen"));
    }

    @Test(priority = 11, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证round函数只有一个x参数，默认按0位小数返回")
    public void test12RoundXOnly(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outResult");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.roundXOnly(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证ceiling函数，正常返回不小于x的最小整数值")
    public void test13CeilingPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.ceilingPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 13, enabled = true, expectedExceptions = SQLException.class, description = "验证ceiling函数参数个数不符，预期失败")
    public void test14CeilingWrongArg(Map<String, String> param) throws SQLException {
        numericObj.ceilingWrongArg(param.get("ceilingState"));
    }

    @Test(priority = 14, enabled = true, expectedExceptions = SQLException.class, description = "验证ceiling函数参数为字符串，预期失败")
    public void test15CeilingStrArg(Map<String, String> param) throws SQLException {
        numericObj.ceilingStrArg(param.get("inputNum"));
    }

    @Test(priority = 15, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证ceil函数，同ceiling函数")
    public void test16CeilFunc(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.ceilFunc(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 16, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证floor函数，正常返回不大于x的最大整数值")
    public void test17FloorPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.floorPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 17, enabled = true, expectedExceptions = SQLException.class, description = "验证floor函数参数个数不符，预期失败")
    public void test18FloorWrongArg(Map<String, String> param) throws SQLException {
        numericObj.floorWrongArg(param.get("floorState"));
    }

    @Test(priority = 18, enabled = true, expectedExceptions = SQLException.class, description = "验证floor函数参数为字符串，预期失败")
    public void test19FloorStrArg(Map<String, String> param) throws SQLException {
        numericObj.floorStrArg(param.get("inputNum"));
    }

    @Test(priority = 19, enabled = true, dataProvider = "yamlNumericFuncMethod", description = "验证floor函数，正常返回参数绝对值")
    public void test20ABSPositiveArg(Map<String, String> param) throws SQLException {
        String exepectedRes = param.get("outNum");
        System.out.println("Expected: " + exepectedRes);

        String actualRes = numericObj.absPositiveArg(param.get("inputNum"));
        System.out.println("Actual: " + actualRes);
        Assert.assertEquals(actualRes, exepectedRes);
    }

    @Test(priority = 20, enabled = true, expectedExceptions = SQLException.class, description = "验证abs函数参数个数不符，预期失败")
    public void test21ABSWrongArg(Map<String, String> param) throws SQLException {
        numericObj.absWrongArg(param.get("absState"));
    }

    @Test(priority = 21, enabled = true, expectedExceptions = SQLException.class, description = "验证abs函数参数为字符串，预期失败")
    public void test22ABSStrArg(Map<String, String> param) throws SQLException {
        numericObj.absStrArg(param.get("inputNum"));
    }


}
