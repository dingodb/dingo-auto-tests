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

import io.dingodb.dailytest.StrFuncs;
import listener.EmailableReporterListener;
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
                "ijing changyang","anghai","han","njing","ijing chaoyang","3","ANGping","ong qing ","tp://WWW.baidu.com"};
        for (int i=0; i < char_lengthArray.length; i++){
            expectedChar_lengthList.add(char_lengthArray[i]);
        }
        System.out.println("期望输出列表：" + expectedChar_lengthList);

        List<String> actualChar_lengthInFuncList = strObj.charlengthInStrFunc();
        System.out.println("实际输出列表：" + actualChar_lengthInFuncList);

        Assert.assertTrue(actualChar_lengthInFuncList.equals(expectedChar_lengthList));
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证拼接单个字段，预期异常")
    public void testConcatCase079() throws SQLException, ClassNotFoundException {
        strObj.concatCase079();
    }

    @Test(enabled = true, description = "验证concat拼接按条件查询结果")
    public void testConcatCase083() throws SQLException, ClassNotFoundException {
        List expectedList = new ArrayList();
        expectedList.add("1235440.0");
        expectedList.add("yamaha762.3");
        expectedList.add("  aB c  dE 6199.9999");
        expectedList.add("zhngsna9932.0");
        System.out.println("期望输出列表：" + expectedList);

        List actualConcatList = strObj.concatCase083();
        System.out.println("实际输出列表：" + actualConcatList);
        Assert.assertEquals(actualConcatList, expectedList);
    }

    @Test(enabled = true, description = "验证concat直接连接字符串")
    public void testConcatCase084() throws SQLException, ClassNotFoundException {
        List expectedList = new ArrayList();
        expectedList.add("Hello World123");
        expectedList.add("");
        expectedList.add("01");
        expectedList.add("http://www.baidu.com");
        expectedList.add("http\\n.baidu\\t.com");
        expectedList.add("abc~!@#$%^&*()_+=-|][}{;:/.,?><");
        expectedList.add(null);
        System.out.println("期望输出列表：" + expectedList);

        List actualConcatList = strObj.concatCase084();
        System.out.println("实际输出列表：" + actualConcatList);
        Assert.assertEquals(actualConcatList, expectedList);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证拼接不存在的字段，预期异常")
    public void testConcatCase085() throws SQLException, ClassNotFoundException {
        strObj.concatCase085();
    }

    @Test(enabled = true, description = "验证使用concat拼接")
    public void testConcatFuncUsingConcat() throws SQLException, ClassNotFoundException {
        List expectedList = new ArrayList();
        expectedList.add("Hello World");
        expectedList.add("http://www.baidu.com");
        System.out.println("期望输出列表：" + expectedList);

        List actualConcatList = strObj.concatFuncUsingConcat();
        System.out.println("实际输出列表：" + actualConcatList);
        Assert.assertEquals(actualConcatList, expectedList);
    }

    @Test(enabled = true, description = "验证format函数对整型字段保留大于0位小数")
    public void testFormatCase087() throws SQLException, ClassNotFoundException {
        List expectedFormatList = new ArrayList();
        String[] formatArray = new String[]{"18.00","25.00","55.00","57.00","1.00","544.00","76.00","18.00",
                "76.00","256.00","61.00","2.00","57.00","99.00","18.00"};
        for (int i=0; i < formatArray.length; i++){
            expectedFormatList.add(formatArray[i]);
        }
        System.out.println("期望输出列表：" + expectedFormatList);

        List actualFormatList = strObj.formatCase087();
        System.out.println("实际输出列表：" + actualFormatList);

        Assert.assertTrue(actualFormatList.equals(expectedFormatList));
    }

    @Test(enabled = true, description = "验证format函数对整型字段保留0位小数")
    public void testFormatCase088() throws SQLException, ClassNotFoundException {
        List expectedFormatList = new ArrayList();
        String[] formatArray = new String[]{"18","25","55","57","1","544","76","18","76","256","61","2","57","99","18"};
        for (int i=0; i < formatArray.length; i++){
            expectedFormatList.add(formatArray[i]);
        }
        System.out.println("期望输出列表：" + expectedFormatList);

        List actualFormatList = strObj.formatCase088();
        System.out.println("实际输出列表：" + actualFormatList);

        Assert.assertTrue(actualFormatList.equals(expectedFormatList));
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证格式化字符串类型字段，预期异常")
    public void testFormatCase089() throws SQLException, ClassNotFoundException {
        strObj.formatCase089();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证格式化不存在的字段，预期异常")
    public void testFormatCase090() throws SQLException, ClassNotFoundException {
        strObj.formatCase090();
    }

    @Test(enabled = true, description = "验证format函数对浮点型字段保留0位小数")
    public void testFormatCase091() throws SQLException, ClassNotFoundException {
        List expectedFormatList = new ArrayList();
        String[] formatArray = new String[]{"24","895","123","9","1454","0","2","12","109","1234","100","2345","9","32","0"};
        for (int i=0; i < formatArray.length; i++){
            expectedFormatList.add(formatArray[i]);
        }
        System.out.println("期望输出列表：" + expectedFormatList);
        List actualFormatList = strObj.formatCase091();
        System.out.println("实际输出列表：" + actualFormatList);

        Assert.assertTrue(actualFormatList.equals(expectedFormatList));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证format函数对不同数值的格式化")
    public void testFormatCase092(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedFormatValue = param.get("formatResult");
        System.out.println("Expected：" + expectedFormatValue);
        String actualFormatValue = strObj.formatCase092(param.get("formatValue"), param.get("decimalNum"));
        System.out.println("Actual：" + actualFormatValue);

        Assert.assertTrue(actualFormatValue.equals(expectedFormatValue));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证locate函数子串的查找，返回下标")
    public void testLocateCase096(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedLocateValue = param.get("locateResult");
        System.out.println("Expected：" + expectedLocateValue);
        String actualLocateValue = strObj.locateCase096(param.get("subStr"), param.get("locateStr"));
        System.out.println("Actual：" + actualLocateValue);

        Assert.assertEquals(actualLocateValue, expectedLocateValue);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证locate函数子串为整型数字")
    public void testLocateCase098(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedLocateValue = param.get("locateResult");
        System.out.println("Expected：" + expectedLocateValue);
        String actualLocateValue = strObj.locateCase098(param.get("subStr"), param.get("locateStr"));
        System.out.println("Actual：" + actualLocateValue);

        Assert.assertEquals(actualLocateValue, expectedLocateValue);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证locate函数子串和父串均为整型数字")
    public void testLocateCase099(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedLocateValue = param.get("locateResult");
        System.out.println("Expected：" + expectedLocateValue);
        String actualLocateValue = strObj.locateCase099(param.get("subStr"), param.get("locateStr"));
        System.out.println("Actual：" + actualLocateValue);

        Assert.assertEquals(actualLocateValue, expectedLocateValue);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证locate函数子串为字符，父串为整型数字")
    public void testLocateCase101(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedLocateValue = param.get("locateResult");
        System.out.println("Expected：" + expectedLocateValue);
        String actualLocateValue = strObj.locateCase101(param.get("subStr"), param.get("locateStr"));
        System.out.println("Actual：" + actualLocateValue);

        Assert.assertEquals(actualLocateValue, expectedLocateValue);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证locate函数不支持指定position参数")
    public void testLocateCase112() throws SQLException, ClassNotFoundException {
        strObj.locateCase112();
    }

    @Test(enabled = true, description = "验证locate函数在条件语句中使用")
    public void testLocateCase113() throws SQLException, ClassNotFoundException {
        List expectedLocateList = new ArrayList();
        String[] formatArray = new String[]{"1beijing","2shanghai","3beijing","4shanghai","5beijing",
                "6shanghai","7shanghai","8shanghai","9shanghai","10shanghai","11shanghai"};
        for (int i=0; i < formatArray.length; i++){
            expectedLocateList.add(formatArray[i]);
        }
        System.out.println("期望输出列表：" + expectedLocateList);
        List actualLocateList = strObj.locateCase113();
        System.out.println("实际输出列表：" + actualLocateList);

        Assert.assertTrue(actualLocateList.containsAll(expectedLocateList));
        Assert.assertTrue(expectedLocateList.containsAll(actualLocateList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = strObj.getStrTableName();
        Statement tearDownStatement = connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.execute("delete from tableStrCase113");
        tearDownStatement.execute("drop table tableStrCase113");
        tearDownStatement.close();
        connection.close();
    }

}
