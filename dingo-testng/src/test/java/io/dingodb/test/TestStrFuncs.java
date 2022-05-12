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
import utils.YamlDataHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Listeners(EmailableReporterListener.class)
public class TestStrFuncs extends YamlDataHelper {
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

    @Test(enabled = true, description = "验证格式化数值")
    public void test02FormatStr() throws SQLException, ClassNotFoundException {
        List<String> expectedFormatList = new ArrayList<>();
        String[] formatArray = new String[]{"23.50","895.00","123.12","9.08","1454.00","0.00","2.30","12.30",
                "109.33","1234.46","100.00","2345.00","9.08","32.00","0.12"};
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
        String[] lowerArray = new String[]{"zhangsan","lisi","l3","haha","awjds","123","yamaha","zhangsan",
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
        String[] upperArray = new String[]{"ZHANGSAN", "LISI", "L3", "HAHA", "AWJDS", "123", "YAMAHA", "ZHANGSAN",
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
        String[] leftArray = new String[]{"zha","lis","l3","HAH","awJ","123","yam","zha","op ","lis","  a",
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
                "999", "5.0", "556", "2.0", "235","l3"};
        for (int i=0; i < rightArray.length; i++){
            expectedRightList.add(rightArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedRightList);

        List<String> actualRightList = strObj.rightFunc();
        System.out.println("实际输出列表为：" + actualRightList);
        Assert.assertTrue(actualRightList.equals(expectedRightList));
    }

    @Test(enabled = true, description = "验证复制字符串")
    public void test08RepeatStr() throws SQLException, ClassNotFoundException {
        List<String> expectedRepeatList = new ArrayList<>();
        String[] repeatArray = new String[]{"123123123","CHANGpingCHANGpingCHANGping","chong qing chong qing chong qing ",
                "http://WWW.baidu.comhttp://WWW.baidu.comhttp://WWW.baidu.com","555555"};
        for (int i=0; i < repeatArray.length; i++){
            expectedRepeatList.add(repeatArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedRepeatList);

        List<String> actualRepeatList = strObj.repeatFunc();
        System.out.println("实际输出列表为：" + actualRepeatList);
        Assert.assertTrue(actualRepeatList.equals(expectedRepeatList));
    }

    @Test(enabled = true, description = "验证字符串替换")
    public void test09ReplaceStr() throws SQLException, ClassNotFoundException {
        List<String> expectedReplaceList = new ArrayList<>();
        String[] replaceArray = new String[]{"BJ"," BJ haidian ","wuhan NO.1 Street","CHANGping","pingYang1","543"};
        for (int i=0; i < replaceArray.length; i++){
            expectedReplaceList.add(replaceArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedReplaceList);

        List<String> actualReplaceList = strObj.replaceFunc();
        System.out.println("实际输出列表为：" + actualReplaceList);
        Assert.assertTrue(actualReplaceList.equals(expectedReplaceList));
    }

    @Test(enabled = true, description = "验证去除字符串两边空格")
    public void test10TrimStr() throws SQLException, ClassNotFoundException {
        List<String> expectedTrimList = new ArrayList<>();
        String[] trimArray = new String[]{"op","lisi","aB c  dE","abcdef","HAHA","zhngsna","1.5"};
        for (int i=0; i < trimArray.length; i++){
            expectedTrimList.add(trimArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedTrimList);

        List<String> actualTrimList = strObj.trimFunc();
        System.out.println("实际输出列表为：" + actualTrimList);
        Assert.assertTrue(actualTrimList.equals(expectedTrimList));
    }

    @Test(enabled = true, description = "验证去除字符串左边空格")
    public void test11LtrimStr() throws SQLException, ClassNotFoundException {
        List<String> expectedLtrimList = new ArrayList<>();
        String[] ltrimArray = new String[]{"op ","lisi","aB c  dE ","abcdef","HAHA","zhngsna","1.5"};
        for (int i=0; i < ltrimArray.length; i++){
            expectedLtrimList.add(ltrimArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedLtrimList);

        List<String> actualLtrimList = strObj.ltrimFunc();
        System.out.println("实际输出列表为：" + actualLtrimList);
        Assert.assertTrue(actualLtrimList.equals(expectedLtrimList));
    }

    @Test(enabled = true, description = "验证去除字符串右边空格")
    public void test12RtrimStr() throws SQLException, ClassNotFoundException {
        List<String> expectedRtrimList = new ArrayList<>();
        String[] rtrimArray = new String[]{"op","lisi","  aB c  dE"," abcdef","HAHA","zhngsna","1.5"};
        for (int i=0; i < rtrimArray.length; i++){
            expectedRtrimList.add(rtrimArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedRtrimList);

        List<String> actualRtrimList = strObj.rtrimFunc();
        System.out.println("实际输出列表为：" + actualRtrimList);
        Assert.assertTrue(actualRtrimList.equals(expectedRtrimList));
    }

    @Test(enabled = true, description = "验证mid截取指定长度字符串")
    public void test13MidStr() throws SQLException, ClassNotFoundException {
        List<String> expectedMidList = new ArrayList<>();
        String[] midArray = new String[]{"han","isi","3","AHA","wJD","23","ama","han","p ","isi"," aB","abc","AHA","hng",".5"};
        for (int i=0; i < midArray.length; i++){
            expectedMidList.add(midArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedMidList);

        List<String> actualMidList = strObj.midFunc();
        System.out.println("实际输出列表为：" + actualMidList);
        Assert.assertTrue(actualMidList.equals(expectedMidList));
    }

    @Test(enabled = true, description = "验证mid截取字符串，无指定长度")
    public void test13MidStrWithoutLengthArg() throws SQLException, ClassNotFoundException {
        List<String> expectedMidWithoutLengthList = new ArrayList<>();
        String[] midWithoutLengthArray = new String[]{"hangsan","isi","3","AHA","wJDs","23","amaha","hangsan","p ","isi"," aB c  dE ","abcdef","AHA","hngsna",".5"};
        for (int i=0; i < midWithoutLengthArray.length; i++){
            expectedMidWithoutLengthList.add(midWithoutLengthArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedMidWithoutLengthList);

        List<String> actualMidWithoutLengthList = strObj.midWithoutLengthArgFunc();
        System.out.println("实际输出列表为：" + actualMidWithoutLengthList);
        Assert.assertTrue(actualMidWithoutLengthList.equals(expectedMidWithoutLengthList));
    }


    @Test(enabled = true, description = "验证subString截取指定长度字符串")
    public void test14SubStringStr() throws SQLException, ClassNotFoundException {
        List<String> expectedSubStringList = new ArrayList<>();
        String[] subStringArray = new String[]{"beijing"," beijing ha","wuhan NO.1 ","CHANGping","pingYang1","543",
                "beijing cha","shanghai","wuhan","nanjing","beijing cha","123","CHANGping","chong qing ","http://WWW."};
        for (int i=0; i < subStringArray.length; i++){
            expectedSubStringList.add(subStringArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedSubStringList);

        List<String> actualSubStringList = strObj.subStringFunc();
        System.out.println("实际输出列表为：" + actualSubStringList);
        Assert.assertTrue(actualSubStringList.equals(expectedSubStringList));
    }

    @Test(enabled = true, description = "验证字符串反转")
    public void test15ReverseStr() throws SQLException, ClassNotFoundException {
        List<String> expectedReverseList = new ArrayList<>();
        String[] reverseArray = new String[]{"321","gnipGNAHC"," gniq gnohc","moc.udiab.WWW//:ptth"};
        for (int i=0; i < reverseArray.length; i++){
            expectedReverseList.add(reverseArray[i]);
        }
        System.out.println("期望输出列表为：" + expectedReverseList);

        List<String> actualReverseList = strObj.reverseFunc();
        System.out.println("实际输出列表为：" + actualReverseList);
        Assert.assertTrue(actualReverseList.equals(expectedReverseList));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod", description = "验证获取字符串参数字符长度")
    public void test16Char_LengthStr(Map<String, String> param) throws SQLException, ClassNotFoundException {
        int expectedCharLength = Integer.valueOf(param.get("outputlen"));
        System.out.println("Expected: " + expectedCharLength);
        int actualCharLength = strObj.charlengthStr(param.get("paramstr"));
        System.out.println("Actual: " + actualCharLength);

        Assert.assertEquals(actualCharLength, expectedCharLength);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod", description = "验证获取非字符串参数字符长度")
    public void test17Char_LengthNonStr(Map<String, String> param) throws SQLException, ClassNotFoundException {
        int expectedCharLength = Integer.valueOf(param.get("outputlen"));
        System.out.println("Expected: " + expectedCharLength);
        int actualCharLength = strObj.charlengthNonStr(param.get("paramstr"));
        System.out.println("Actual: " + actualCharLength);

        Assert.assertEquals(actualCharLength, expectedCharLength);
    }

    @Test(enabled = true, description = "验证参数为null返回null")
    public void test18Char_LengthNull() throws SQLException, ClassNotFoundException {
        Object actualCharLength = strObj.charlengthNull();
        System.out.println("Actual: " + actualCharLength);

        Assert.assertNull(actualCharLength);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证参数为空，执行异常")
    public void test19Char_LengthBlankParam() throws SQLException, ClassNotFoundException {
        Integer actualCharLength = strObj.charlengthBlankParam();
    }

    @Test(enabled = true, description = "验证char_length函数在表格中使用")
    public void test20Char_lengthInTable() throws SQLException, ClassNotFoundException {
        Integer[][] charLengthArray = {{8,7,2,4},{4,17,2,5}, {2,17,2,7},{4,9,2,9},{5,9,1,9},{3,3,3,3},{6,17,2,3},
                {8,8,2,4},{3,5,2,7},{4,7,3,8},{11,16,2,7},{7,3,1,6},{4,9,2,9},{7,11,2,4},{3,20,2,6}};
        List<List<Integer>> expectedChar_LengthList = new ArrayList<List<Integer>>();
        for(int i=0; i<charLengthArray.length; i++) {
            List<Integer> columnList = new ArrayList<>();
            for (int j=0; j<charLengthArray[i].length; j++) {
                columnList.add(charLengthArray[i][j]);
            }
            expectedChar_LengthList.add(columnList);
        }
        System.out.println("Expected: " + expectedChar_LengthList);
        List<List<Integer>> actualChar_LengthList = strObj.charlengthInTable();
        System.out.println("Actual: " + actualChar_LengthList);

        Assert.assertTrue(actualChar_LengthList.containsAll(expectedChar_LengthList));
        Assert.assertTrue(expectedChar_LengthList.containsAll(actualChar_LengthList));
    }

    @Test(enabled = true, description = "验证char_length在其他字符串函数中使用")
    public void test21Char_lengthInStrFunc() throws SQLException, ClassNotFoundException {
        List<String> expectedChar_lengthList = new ArrayList<String>();
        String[] char_lengthArray = new String[]{"ijing","eijing haidian ","han NO.1 Street","ANGping","ngYang1","3",
                "ijing changyang","anghai","han","njing","ijing chaoyang","3","ANGping","ong qing ","tp://WWW.baidu.com",};
        for (int i=0; i < char_lengthArray.length; i++){
            expectedChar_lengthList.add(char_lengthArray[i]);
        }
        System.out.println("期望输出列表：" + expectedChar_lengthList);

        List<String> actualChar_lengthInFuncList = strObj.charlengthInStrFunc();
        System.out.println("实际输出列表：" + actualChar_lengthInFuncList);

        Assert.assertTrue(actualChar_lengthInFuncList.equals(expectedChar_lengthList));
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
