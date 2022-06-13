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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import utils.YamlDataHelper;

import java.sql.Connection;
import java.sql.SQLException;
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
        List<List> expectedReverseList = new ArrayList<List>();
        String[][] reverseArray = {{"321","2"},{"gnipGNAHC","75"},{" gniq gnohc","99"},{"moc.udiab.WWW//:ptth","81"}};

        for(int i=0; i<reverseArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<reverseArray[i].length; j++) {
                columnList.add(reverseArray[i][j]);
            }
            expectedReverseList.add(columnList);
        }
        System.out.println("Expected: " + expectedReverseList);

        List<List> actualReverseList = strObj.reverseFunc();
        System.out.println("实际输出列表为：" + actualReverseList);
        Assert.assertTrue(actualReverseList.containsAll(expectedReverseList));
        Assert.assertTrue(expectedReverseList.containsAll(actualReverseList));
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

    @Test(enabled = true, description = "验证拼接空表，返回空")
    public void testConcatCase081() throws SQLException, ClassNotFoundException {
        Boolean expectedReturn = false;
        System.out.println("Expected: " + expectedReturn);
        Boolean actualReturn = strObj.concatCase081();

        Assert.assertFalse(actualReturn);
    }

    @Test(enabled = true, dependsOnMethods = {"testConcatCase081"}, description = "验证concat表格里使用")
    public void testConcatCase077() throws SQLException, ClassNotFoundException {
        String[][] concatArray = {{"1Haha10023.45BJ","10023.45","BJ23.45"},{null,"5018.9","SH18.9"},
                {null,"00.0",null},{null,"00.01",null},{null,null,null},};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<concatArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<concatArray[i].length; j++) {
                columnList.add(concatArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);

        List<List> actualConcatList = strObj.concatCase077();
        System.out.println("Actual: "+ actualConcatList);
        Assert.assertTrue(actualConcatList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualConcatList));
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

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证lower函数功能")
    public void testLowerCase119(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedLowerValue = param.get("outputCase");
        System.out.println("Expected：" + expectedLowerValue);
        String actualLowerValue = strObj.lowerCase119(param.get("inputCase"));
        System.out.println("Actual：" + actualLowerValue);

        Assert.assertEquals(actualLowerValue, expectedLowerValue);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证upper函数功能")
    public void testUpperCase125(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedUpperValue = param.get("outputCase");
        System.out.println("Expected：" + expectedUpperValue);
        String actualUpperValue = strObj.upperCase125(param.get("inputCase"));
        System.out.println("Actual：" + actualUpperValue);

        Assert.assertEquals(actualUpperValue, expectedUpperValue);
    }

    @Test(enabled = true,description = "验证lower和upper函数查询起别名")
    public void testLUCase126() throws SQLException, ClassNotFoundException {
        String expectedStr = "abc1DEF2";
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.lowerUpperCase126();
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证left函数截取字符串")
    public void testLeftCase127(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("outputStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.leftCase127(param.get("inputStr"), param.get("inputLen"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证left函数截取数值")
    public void testLeftCase130(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("outputStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.leftCase130(param.get("inputStr"), param.get("inputLen"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证left函数接收参数不符,预期异常")
    public void testLeftCase133_1() throws SQLException, ClassNotFoundException {
        strObj.leftCase133_1();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证left函数接收参数不符，预期异常")
    public void testLeftCase133_2() throws SQLException, ClassNotFoundException {
        strObj.leftCase133_2();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证left函数接收参数不符，预期异常")
    public void testLeftCase134_1() throws SQLException, ClassNotFoundException {
        strObj.leftCase134_1();
    }

    @Test(enabled = true, description = "验证left函数在表格中使用")
    public void testLeftCase135() throws SQLException, ClassNotFoundException {
        String[][] leftArray = {{"zha","beijing","1","23.5"},{"lis","nanjing","2","1234"},{"1.5","http://WWW","1","0.12"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<leftArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<leftArray[i].length; j++) {
                columnList.add(leftArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);

        List<List> actualLeftList = strObj.leftCase135();
        System.out.println("Actual: "+ actualLeftList);
        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证right函数截取字符串")
    public void testRightCase138(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("outputStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.rightCase138(param.get("inputStr"), param.get("inputLen"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证right函数截取数字")
    public void testRightCase141(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("outputStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.rightCase141(param.get("inputStr"), param.get("inputLen"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证right函数接收参数不符,预期异常")
    public void testRightCase144_1() throws SQLException, ClassNotFoundException {
        strObj.rightCase144_1();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证right函数接收参数不符，预期异常")
    public void testRightCase144_2() throws SQLException, ClassNotFoundException {
        strObj.rightCase144_2();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证right函数接收参数不符，预期异常")
    public void testRightCase145() throws SQLException, ClassNotFoundException {
        strObj.rightCase145();
    }

    @Test(enabled = true, description = "验证right函数在表格中使用")
    public void testRightCase146() throws SQLException, ClassNotFoundException {
        String[][] rightArray = {{"beijing","23.5","18"},{"g haidian ","95.0","25"},{".baidu.com","1235","18"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<rightArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<rightArray[i].length; j++) {
                columnList.add(rightArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);

        List<List> actualRightList = strObj.rightCase146();
        System.out.println("Actual: "+ actualRightList);
        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(enabled = true, description = "验证left，right函数和拼接函数一起使用")
    public void testLeftRightCase149() throws SQLException, ClassNotFoundException {
        List expectedList = new ArrayList();
        expectedList.add("Hello World");
        expectedList.add("test Dingo");
        expectedList.add("Hello+World");
        expectedList.add("test-go");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = strObj.leftRightCase149();
        System.out.println("Actual: "+ actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证repeat复制字符串")
    public void testRepeatCase148(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("repeatOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.repeatCase148(param.get("repeatStr"), param.get("repeatNum"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证repeat复制数值")
    public void testRepeatCase157(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("repeatOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.repeatCase157(param.get("repeatStr"), param.get("repeatNum"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, description = "验证多个repeat使用")
    public void testRepeatCase160() throws SQLException, ClassNotFoundException {
        List expectedList = new ArrayList();
        expectedList.add("abcabcabc");
        expectedList.add("123123");
        expectedList.add("1.10");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = strObj.repeatCase160();
        System.out.println("Actual: "+ actualList);
        Assert.assertEquals(actualList, expectedList);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证repeat函数接收参数不符,预期异常")
    public void testRepeatCase161_1() throws SQLException, ClassNotFoundException {
        strObj.repeatCase161_1();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证repeat函数接收参数不符，预期异常")
    public void testRepeatCase161_2() throws SQLException, ClassNotFoundException {
        strObj.repeatCase161_2();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证repeat函数复制参数为非数字")
    public void testRepeatCase162() throws SQLException, ClassNotFoundException {
        strObj.repeatCase162();
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证replace函数替换字符串")
    public void testReplaceCase163(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("replaceOutStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.replaceCase163(param.get("replaceParentStr"), param.get("replaceSubStr"), param.get("replaceStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证replace函数替换数值")
    public void testReplaceCase170(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("replaceOutStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.replaceCase170(param.get("replaceParentStr"), param.get("replaceSubStr"), param.get("replaceStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证replace函数接收参数不符，预期异常")
    public void testReplaceCase172_1() throws SQLException, ClassNotFoundException {
        strObj.replaceCase172_1();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证replac函数接收参数不符，预期异常")
    public void testReplaceCase172_2() throws SQLException, ClassNotFoundException {
        strObj.replaceCase172_2();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证replace函数接收参数不符，预期异常")
    public void testReplaceCase172_3() throws SQLException, ClassNotFoundException {
        strObj.replaceCase172_3();
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, description = "验证replac函数接收参数不符，预期异常")
    public void testReplaceCase172_4() throws SQLException, ClassNotFoundException {
        strObj.replaceCase172_4();
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证replace函数替换字符串为数值")
    public void testReplaceCase173(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("replaceOutStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.replaceCase173(param.get("replaceParentStr"), param.get("replaceSubStr"), param.get("replaceStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证replace函数替换数值为字符串")
    public void testReplaceCase175(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("replaceOutStr");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.replaceCase175(param.get("replaceParentStr"), param.get("replaceSubStr"), param.get("replaceStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, description = "验证replace在update语句中使用")
    public void testReplaceCase177() throws SQLException, ClassNotFoundException {
        List expectedReplaceList = new ArrayList();
        expectedReplaceList.add(4);
        String[] replaceArray = new String[]{"1shanghai","2haidian","3 shanghai chaoyang ","4chaoyangdis_1 NO.street",
                "5shanghai", "6shanghai changping 89","7haidian2","8wuwuxi ","9  maya","10",null};
        for (int i=0; i < replaceArray.length; i++){
            expectedReplaceList.add(replaceArray[i]);
        }
        System.out.println("期望输出列表：" + expectedReplaceList);
        List actualReplaceList = strObj.replaceCase177();
        System.out.println("实际输出列表：" + actualReplaceList);

        Assert.assertTrue(actualReplaceList.containsAll(expectedReplaceList));
        Assert.assertTrue(expectedReplaceList.containsAll(actualReplaceList));
    }

    @Test(enabled = true, dependsOnMethods = {"testReplaceCase177"},
            description = "验证replace在update语句中使用，替换浮点型字段值为整型字段值")
    public void testReplaceCase178() throws SQLException, ClassNotFoundException {
        List expectedReplaceList = new ArrayList();
        String[] replaceArray = new String[]{"1-18.0","3-35.0","5-18.0","7-99.0","9-28.0"};
        for (int i=0; i < replaceArray.length; i++){
            expectedReplaceList.add(replaceArray[i]);
        }
        System.out.println("期望输出列表：" + expectedReplaceList);
        List actualReplaceList = strObj.replaceCase178();
        System.out.println("实际输出列表：" + actualReplaceList);

        Assert.assertTrue(actualReplaceList.containsAll(expectedReplaceList));
        Assert.assertTrue(expectedReplaceList.containsAll(actualReplaceList));
    }

    @Test(enabled = true, dependsOnMethods = {"testReplaceCase178"},
            description = "验证replace在update语句中使用,替换整型字段值为浮点型字段值")
    public void testReplaceCase179_1() throws SQLException, ClassNotFoundException {
        int actualReplaceRows = strObj.replaceCase179_1();
        System.out.println("实际更新行数：" + actualReplaceRows);

        Assert.assertEquals(actualReplaceRows, 4);
    }

    @Test(enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"testReplaceCase179_1"},
            description = "验证replac函数在update语句中使用，预期异常")
    public void testReplaceCase179_2() throws SQLException, ClassNotFoundException {
        int actualReplaceRows = strObj.replaceCase179_2();
        System.out.println("实际更新行数：" + actualReplaceRows);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数去除字符串两边空格")
    public void testTrimCase181(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase181(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数leading用法去除字符串左边空格")
    public void testTrimCase182(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase182(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数trailing用法去除字符串右边空格")
    public void testTrimCase183(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase183(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数both去除字符串两边空格")
    public void testTrimCase184(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase184(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数leading去除左边指定字符")
    public void testTrimCase185(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase185(param.get("trimStr"), param.get("leadingCha"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数trailing去除右边指定字符")
    public void testTrimCase186(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase186(param.get("trimStr"), param.get("trailingCha"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数both去除两边指定字符")
    public void testTrimCase187(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase187(param.get("trimStr"), param.get("bothCha"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数默认按both去除两边指定字符")
    public void testTrimCase188(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase188(param.get("trimStr"), param.get("bothCha"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证trim函数用于数值")
    public void testTrimCase192(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.trimCase192(param.get("trimState"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证ltrim函数去除字符串左边空格")
    public void testLtrimCase194(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.ltrimCase194(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证rtrim函数去除字符串右边空格")
    public void testRtrimCase196(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("trimOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.rtrimCase196(param.get("trimStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod", expectedExceptions = SQLException.class,description = "验证ltrim和rtrim函数不支持去除指定字符")
    public void testTrimCase195(Map<String, String> param) throws SQLException, ClassNotFoundException {
        strObj.trimCase195(param.get("trimState"));
    }

    @Test(enabled = true, description = "验证trim在表中使用")
    public void testTrimCase198() throws SQLException, ClassNotFoundException {
        List expectedTrimList = new ArrayList();
        String[] trimArray = new String[]{"TAT","TATtt","18",".","aabcaa "," aabcaa","aabcaa"};
        for (int i=0; i < trimArray.length; i++){
            expectedTrimList.add(trimArray[i]);
        }
        System.out.println("期望输出列表：" + expectedTrimList);
        List actualTrimList = strObj.trimCase198();
        System.out.println("实际输出列表：" + actualTrimList);

        Assert.assertEquals(actualTrimList, expectedTrimList);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证mid函数截取字符串")
    public void testMidCase201(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("midOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.midCase201(param.get("midStr"), param.get("midStartIndex"), param.get("midLength"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证mid函数参数不符，预期异常")
    public void testMidCase207(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.midCase201(param.get("midStr"), param.get("midStartIndex"), param.get("midLength"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证mid函数截取数字")
    public void testMidCase210_1(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("midOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.midCase210(param.get("midStr"), param.get("midStartIndex"), param.get("midLength"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证mid函数参数不符，预期异常")
    public void testMidCase210_2(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.midCase210(param.get("midStr"), param.get("midStartIndex"), param.get("midLength"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证mid函数参数不符，预期异常")
    public void testMidCase221(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.midCase221(param.get("midState"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证mid函数省略截取长度参数")
    public void testMidCase212(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("midOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.midCase212(param.get("midStr"), param.get("midStartIndex"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证subString函数截取字符串")
    public void testSubStringCase222(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("subOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.subStringCase222(param.get("subStr"), param.get("subStartIndex"), param.get("subLength"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证subString函数参数不符，预期异常")
    public void testSubStringCase227(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.subStringCase222(param.get("subStr"), param.get("subStartIndex"), param.get("subLength"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证subString函数截取数值")
    public void testSubStringCase231_1(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("subOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.subStringCase231(param.get("subStr"), param.get("subStartIndex"), param.get("subLength"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证subString函数参数不符，预期异常")
    public void testSubStringCase231_2(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.subStringCase231(param.get("subStr"), param.get("subStartIndex"), param.get("subLength"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证subString函数参数不符，预期异常")
    public void testSubStringCase238(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.subStringCase238(param.get("subState"));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证subString函数支持from x for y用法")
    public void testSubStringCase241(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("subOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.subStringCase241(param.get("subStr"), param.get("subStartIndex"), param.get("subLength"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, description = "验证mid,Substring函数在表格中使用")
    public void testSubStringCase246() throws SQLException, ClassNotFoundException {
        String[][] subArray = {{"lisi"," beijing","25"},{"lisi","nanjing","56"},{"1.5","http://W","18"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<subArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<subArray[i].length; j++) {
                columnList.add(subArray[i][j]);
            }
            expectedList.add(columnList);
        }
        System.out.println("Expected: " + expectedList);

        List<List> actualSubList = strObj.subStringCase246();
        System.out.println("Actual: "+ actualSubList);
        Assert.assertTrue(actualSubList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualSubList));
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证reverse函数反转字符串")
    public void testReverseCase248(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("reverseOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.reverseCase248(param.get("reverseStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",description = "验证reverse函数反转数值")
    public void testReverseCase249(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedStr = param.get("reverseOut");
        System.out.println("Expected：" + expectedStr);
        String actualStr = strObj.reverseCase249(param.get("reverseStr"));
        System.out.println("Actual：" + actualStr);

        Assert.assertEquals(actualStr, expectedStr);
    }

    @Test(enabled = true, dataProvider = "yamlStrFuncMethod",expectedExceptions = SQLException.class,
            description = "验证reverse函数参数非法，预期异常")
    public void testReverseCase255(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String actualStr = strObj.reverseCase249(param.get("reverseState"));
    }


//    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
//    public void tearDownAll() throws SQLException {
//        String tableName = strObj.getStrTableName();
//        Statement tearDownStatement = connection.createStatement();
//        tearDownStatement.execute("delete from " + tableName);
//        tearDownStatement.execute("drop table " + tableName);
//        tearDownStatement.execute("delete from tableStrCase113");
//        tearDownStatement.execute("drop table tableStrCase113");
//        tearDownStatement.execute("delete from tableReplaceCase177");
//        tearDownStatement.execute("drop table tableReplaceCase177");
//        tearDownStatement.execute("delete from tableTrimCase198");
//        tearDownStatement.execute("drop table tableTrimCase198");
//        tearDownStatement.execute("delete from tableConcatCase081");
//        tearDownStatement.execute("drop table tableConcatCase081");
//        tearDownStatement.close();
//        connection.close();
//    }

}
