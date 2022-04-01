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

import listener.EmailableReporterListener;
import io.dingodb.dailytest.StrFuncs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Listeners(EmailableReporterListener.class)
public class TestStrFuncs {
    private static Connection connection;
    public static StrFuncs strObj = new StrFuncs();

    @BeforeClass(alwaysRun = true, description = "测试开始前，获取数据库连接创建数据表并插入数据")
    public static void ConnectCreateInsert() throws ClassNotFoundException, SQLException {
        connection = StrFuncs.connectStrDB();
        strObj.createFuncTable();
        int insertRowNum = strObj.insertValues();
        Assert.assertEquals(insertRowNum,15);
    }

    @Test(enabled = true,description = "验证字符串拼接的功能")
    public void test01ConcatStr() throws SQLException, ClassNotFoundException {
        String expectedConcatStr = "zhangsan18beijing";
        String actualConcatStr = strObj.concatFunc();
        Assert.assertEquals(actualConcatStr, expectedConcatStr);
    }

    @Test(enabled = false, description = "验证格式化数值")
    public void test02FormatStr() throws SQLException, ClassNotFoundException {
        List<String> expectedFormatList = new ArrayList<>();
        String[] formatArray = new String[]{"23.50","895.00","123.12","9.08","1,454.00","0.00","2.30","12.30",
                "109.33","1,234.46","100.00","2,345.00","9.08","32.00","0.12"};
        for (int i=0; i < formatArray.length; i++){
            expectedFormatList.add(formatArray[i]);
        }
        System.out.println("期望格式化后的列表为：" + expectedFormatList);

        List<String> actualFormatList = strObj.formatFunc();
        System.out.println("实际格式化后的列表为：" + actualFormatList);
        Assert.assertTrue(actualFormatList.equals(expectedFormatList));
    }

    @Test(enabled = true, description = "验证查找字符串位置")
    public void test03LocateStr() throws SQLException, ClassNotFoundException {
        List<String> expectedLocateList = new ArrayList<>();
        String[] locateArray = new String[]{"3","0","0","0","1","0","2","3","0","0","3","2","0","7","0"};
        for (int i=0; i < locateArray.length; i++){
            expectedLocateList.add(locateArray[i]);
        }
        System.out.println("期望查找的位置列表为：" + expectedLocateList);

        List<String> actualLocateList = strObj.locateFunc();
        System.out.println("实际查找的位置列表为：" + actualLocateList);
        Assert.assertTrue(actualLocateList.equals(expectedLocateList));
    }

    @Test(enabled = true, description = "验证字母转为小写")
    public void test04LowerAndLcase() throws SQLException, ClassNotFoundException {
        List<String> expectedLowerList = new ArrayList<>();
        String[] lowerArray = new String[]{"zhangsan","lisi","lisi3","haha","awjds","123","yamaha","zhangsan",
                "op ","lisi","  ab c  de "," abcdef","haha","zhngsna","1.5","http://www.baidu.com"};
        for (int i=0; i < lowerArray.length; i++){
            expectedLowerList.add(lowerArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedLowerList);

        List<String> actualLowerList = strObj.lowerFunc();
        System.out.println("实际输出列表为：" + actualLowerList);
        Assert.assertTrue(actualLowerList.equals(expectedLowerList));
    }

    @Test(enabled = true, description = "验证字母转为大写")
    public void test05UpperAndUcase() throws SQLException, ClassNotFoundException {
        List<String> expectedUpperList = new ArrayList<>();
        String[] upperArray = new String[]{"ZHANGSAN", "LISI", "LISI3", "HAHA", "AWJDS", "123", "YAMAHA", "ZHANGSAN",
                "OP ", "LISI", "  AB C  DE ", " ABCDEF", "HAHA", "ZHNGSNA", "1.5", "WUHAN NO.1 STREET"};
        for (int i=0; i < upperArray.length; i++){
            expectedUpperList.add(upperArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedUpperList);

        List<String> actualUpperList = strObj.upperFunc();
        System.out.println("实际输出列表为：" + actualUpperList);
        Assert.assertTrue(actualUpperList.equals(expectedUpperList));
    }

    @Test(enabled = true, description = "验证从左边截取字符串")
    public void test06LeftStr() throws SQLException, ClassNotFoundException {
        List<String> expectedLeftList = new ArrayList<>();
        String[] leftArray = new String[]{"zha","lis","lis","HAH","awJ","123","yam","zha","op ","lis","  a",
                " ab","HAH","zhn","1.5"};
        for (int i=0; i < leftArray.length; i++){
            expectedLeftList.add(leftArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedLeftList);

        List<String> actualLeftList = strObj.leftFunc();
        System.out.println("实际输出列表为：" + actualLeftList);
        Assert.assertTrue(actualLeftList.equals(expectedLeftList));
    }

    @Test(enabled = true, description = "验证从右边截取字符串")
    public void test07RightStr() throws SQLException, ClassNotFoundException {
        List<String> expectedRightList = new ArrayList<>();
        String[] rightArray = new String[]{"3.5", "5.0", "123", "556", "999", "0.0", "2.3", "2.3", "325", "456",
                "999", "5.0", "556", "2.0", "235"};
        for (int i=0; i < rightArray.length; i++){
            expectedRightList.add(rightArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedRightList);

        List<String> actualRightList = strObj.rightFunc();
        System.out.println("实际输出列表为：" + actualRightList);
        Assert.assertTrue(actualRightList.equals(expectedRightList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = strObj.getStrTableName();
        Statement tearDownStatement = connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.close();
        connection.close();
    }

}
