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

package io.dingodb.dailytest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class StrFuncs {
//    private static final String defaultConnectIP = "172.20.3.26";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";

    public static Connection connection;

    //连接数据库,返回数据库连接对象
    public static Connection connectStrDB() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(connectUrl);
        return connection;
    }

    //生成测试表格名称并返回
    public static String getStrTableName() {
        final String funcTablePrefix = "strFuncTest";
        String tableName = funcTablePrefix + CommonArgs.getCurDateStr();
        return tableName;
    }

    //创建表格
    public void createFuncTable() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String createTableSQL = "create table " + strFuncTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "primary key(id)"
                + ")";
        statement.execute(createTableSQL);
        statement.close();
    }

    // 插入数据
    public int insertValues() throws ClassNotFoundException, SQLException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String batInsertSql = "insert into " + strFuncTableName +
                " values (1,'zhangsan',18,23.50,'beijing'),\n" +
                "(2,'lisi',25,895,' beijing haidian '),\n" +
                "(3,'l3',55,123.123,'wuhan NO.1 Street'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(5,'awJDs',1,1453.9999,'pingYang1'),\n" +
                "(6,'123',544,0,'543'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang'),\n" +
                "(8,'zhangsan',18,12.3,'shanghai'),\n" +
                "(9,'op ',76,109.325,'wuhan'),\n" +
                "(10,'lisi',256,1234.456,'nanjing'),\n" +
                "(11,'  aB c  dE ',61,99.9999,'beijing chaoyang'),\n" +
                "(12,' abcdef',2,2345.000,'123'),\n" +
                "(13,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(14,'zhngsna',99,32,'chong qing '),\n" +
                "(15,'1.5',18,0.1235,'http://WWW.baidu.com')";
        int insertRows = statement.executeUpdate(batInsertSql);
        statement.close();
        return insertRows;
    }

    //字符串拼接
    public String concatFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String concatSql = "select name||age||address cnaa from " + strFuncTableName + " where id=1";
        ResultSet concatRst = statement.executeQuery(concatSql);
        String concatStr = null;
        while (concatRst.next()) {
            concatStr = concatRst.getString("cnaa");
        }
        statement.close();
        return concatStr;
    }

    //格式化保留小数位
    public List<String> formatFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(amount,2) famount from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        List<String> formatList = new ArrayList<String>();
        while (formatRst.next()){
            formatList.add(formatRst.getString("famount"));
        }
        statement.close();
        return formatList;
    }

    //查找字符串所在位置
    public List<String> locateFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate('a',name) locName from " + strFuncTableName;
        ResultSet locateRst = statement.executeQuery(locateSQL);
        List<String> locateList = new ArrayList<String>();
        while (locateRst.next()){
            locateList.add(locateRst.getString("locName"));
        }
        statement.close();
        return locateList;
    }

    //将字符串中的字母全部变为小写
    public List<String> lowerFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String lowerSQL = "select lower(name) lowName from " + strFuncTableName;
        ResultSet lowerRst = statement.executeQuery(lowerSQL);
        List<String> lowerList = new ArrayList<String>();
        while (lowerRst.next()){
            lowerList.add(lowerRst.getString("lowName"));
        }

        String lcaseSQL = "select lcase(address) lcaAddress from " + strFuncTableName + " where id=15";
        ResultSet lcaseRst = statement.executeQuery(lcaseSQL);
        while (lcaseRst.next()){
            lowerList.add(lcaseRst.getString("lcaAddress"));
        }
        statement.close();
        return lowerList;
    }

    //将字符串中的字母全部变为大写
    public List<String> upperFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String lowerSQL = "select upper(name) upName from " + strFuncTableName;
        ResultSet upperRst = statement.executeQuery(lowerSQL);
        List<String> upperList = new ArrayList<String>();
        while (upperRst.next()){
            upperList.add(upperRst.getString("upName"));
        }

        String ucaseSQL = "select ucase(address) ucaAddress from " + strFuncTableName + " where id=3";
        ResultSet ucaseRst = statement.executeQuery(ucaseSQL);
        while (ucaseRst.next()){
            upperList.add(ucaseRst.getString("ucaAddress"));
        }
        statement.close();
        return upperList;
    }

    //返回字符串左边指定个数的字符
    public List<String> leftFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left(name,3) l3name from " + strFuncTableName;
        ResultSet leftRst = statement.executeQuery(leftSQL);
        List<String> leftList = new ArrayList<String>();
        while (leftRst.next()){
            leftList.add(leftRst.getString("l3name"));
        }
        statement.close();
        return leftList;
    }

    //返回字符串右边指定个数的字符
    public List<String> rightFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right(amount,3) r3amount from " + strFuncTableName;
        ResultSet rightRst = statement.executeQuery(rightSQL);
        List<String> rightList = new ArrayList<String>();
        while (rightRst.next()){
            rightList.add(rightRst.getString("r3amount"));
        }
        ResultSet rightRst2 = statement.executeQuery("select right(name,3) r3name from " + strFuncTableName + " where id=3");
        while (rightRst2.next()){
            rightList.add(rightRst2.getString("r3name"));
        }
        statement.close();
        return rightList;
    }

    //返回字符串重复指定次数的结果
    public List<String> repeatFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String repeatSQL = "select repeat(address,3) reAddress from " + strFuncTableName + " where id>11";
        ResultSet repeatRst = statement.executeQuery(repeatSQL);
        List<String> repeatList = new ArrayList<String>();
        while (repeatRst.next()){
            repeatList.add(repeatRst.getString("reAddress"));
        }
        ResultSet repeatRst2 = statement.executeQuery("select repeat(age,3) reName from " + strFuncTableName + " where id=3");
        while (repeatRst2.next()){
            repeatList.add(repeatRst2.getString("reName"));
        }
        statement.close();
        return repeatList;
    }

    //字符串替换
    public List<String> replaceFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String replaceSQL = "select replace(address,'beijing','BJ') replAddress from " + strFuncTableName + " where id<7";
        ResultSet replaceRst = statement.executeQuery(replaceSQL);
        List<String> replaceList = new ArrayList<String>();
        while (replaceRst.next()){
            replaceList.add(replaceRst.getString("replAddress"));
        }
        statement.close();
        return replaceList;
    }

    //去除字符串两端空格
    public List<String> trimFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String trimSQL = "select trim(name) trName from " + strFuncTableName + " where id>8";
        ResultSet trimRst = statement.executeQuery(trimSQL);
        List<String> trimList = new ArrayList<String>();
        while (trimRst.next()){
            trimList.add(trimRst.getString("trName"));
        }
        statement.close();
        return trimList;
    }

    //去除字符串左侧空格
    public List<String> ltrimFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String ltrimSQL = "select ltrim(name) ltrName from " + strFuncTableName + " where id>8";
        ResultSet ltrimRst = statement.executeQuery(ltrimSQL);
        List<String> ltrimList = new ArrayList<String>();
        while (ltrimRst.next()){
            ltrimList.add(ltrimRst.getString("ltrName"));
        }
        statement.close();
        return ltrimList;
    }

    //去除字符串右侧空格
    public List<String> rtrimFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rtrimSQL = "select rtrim(name) rtrName from " + strFuncTableName + " where id>8";
        ResultSet rtrimRst = statement.executeQuery(rtrimSQL);
        List<String> rtrimList = new ArrayList<String>();
        while (rtrimRst.next()){
            rtrimList.add(rtrimRst.getString("rtrName"));
        }
        statement.close();
        return rtrimList;
    }

    //mid截取字符串
    public List<String> midFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String midSQL = "select mid(name,2,3) midName from " + strFuncTableName;
        ResultSet midRst = statement.executeQuery(midSQL);
        List<String> midList = new ArrayList<String>();
        while (midRst.next()){
            midList.add(midRst.getString("midName"));
        }
        statement.close();
        return midList;
    }

    //mid截取字符串,忽略长度参数
    public List<String> midWithoutLengthArgFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String midWithoutLengthArgSQL = "select mid(name,2) midName from " + strFuncTableName;
        ResultSet midWithoutLengthArgRst = statement.executeQuery(midWithoutLengthArgSQL);
        List<String> midWithoutLengthArgList = new ArrayList<String>();
        while (midWithoutLengthArgRst.next()){
            midWithoutLengthArgList.add(midWithoutLengthArgRst.getString("midName"));
        }
        statement.close();
        return midWithoutLengthArgList;
    }

    //subString截取字符串
    public List<String> subStringFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String subStringSQL = "select subString(address,1,11) subAddr from " + strFuncTableName;
        ResultSet subStringRst = statement.executeQuery(subStringSQL);
        List<String> subStringList = new ArrayList<String>();
        while (subStringRst.next()){
            subStringList.add(subStringRst.getString("subAddr"));
        }
        statement.close();
        return subStringList;
    }

    //reverse反转字符串
    public List<String> reverseFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String reverseSQL = "select reverse(address) revAddr from " + strFuncTableName + " where id>11";
        ResultSet reverseRst = statement.executeQuery(reverseSQL);
        List<String> reverseList = new ArrayList<String>();
        while (reverseRst.next()){
            reverseList.add(reverseRst.getString("revAddr"));
        }
        statement.close();
        return reverseList;
    }

    // 获取字符串参数的字符长度
    public int charlengthStr(String paramStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select char_length('" + paramStr + "')";
        ResultSet char_lengthRst = statement.executeQuery(char_lengthSQL);
        Integer lengthNum = null;

        while(char_lengthRst.next()) {
            lengthNum = char_lengthRst.getInt(1);
        }

        statement.close();
        return lengthNum;
    }

    // 获取非字符串参数的字符长度
    public int charlengthNonStr(String paramStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select char_length(" + paramStr + ")";
        ResultSet char_lengthRst = statement.executeQuery(char_lengthSQL);
        Integer lengthNum = null;

        while(char_lengthRst.next()) {
            lengthNum = char_lengthRst.getInt(1);
        }

        statement.close();
        return lengthNum;
    }
    // 参数为null,char_length返回长度null
    public Object charlengthNull() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select char_length(null)";
        ResultSet char_lengthRst = statement.executeQuery(char_lengthSQL);
        Object lengthNum = null;
        while(char_lengthRst.next()) {
            lengthNum = char_lengthRst.getObject(1);
        }
        statement.close();
        return lengthNum;
    }

    // 参数为空char_length返回异常
    public Integer charlengthBlankParam() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select char_length()";
        ResultSet char_lengthRst = statement.executeQuery(char_lengthSQL);
        Integer lengthNum = null;
        while(char_lengthRst.next()) {
            lengthNum = char_lengthRst.getInt(1);
        }
        statement.close();
        return lengthNum;
    }

    // 表格中使用char_length
    public List<List<Integer>> charlengthInTable() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select char_length(name) cln, char_length(address) cla, " +
                "char_length(age),char_length(amount) from " + strFuncTableName;
        ResultSet char_lengthRst = statement.executeQuery(char_lengthSQL);
        List<List<Integer>> char_lengthInTableList = new ArrayList<List<Integer>>();
        while(char_lengthRst.next()) {
            List<Integer> char_lengthRowList = new ArrayList<Integer>();
            char_lengthRowList.add(char_lengthRst.getInt(1));
            char_lengthRowList.add(char_lengthRst.getInt(2));
            char_lengthRowList.add(char_lengthRst.getInt(3));
            char_lengthRowList.add(char_lengthRst.getInt(4));
            char_lengthInTableList.add(char_lengthRowList);
        }
        statement.close();
        return char_lengthInTableList;
    }

    // 字符串函数中使用char_length
    public List<String> charlengthInStrFunc() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String char_lengthSQL = "select mid(address,3,char_length(address)-2) msub from " + strFuncTableName;
        ResultSet char_lengthInFuncRst = statement.executeQuery(char_lengthSQL);
        List<String> char_lengthInStrFuncList = new ArrayList<String>();
        while(char_lengthInFuncRst.next()) {
            char_lengthInStrFuncList.add(char_lengthInFuncRst.getString("msub"));
        }
        statement.close();
        return char_lengthInStrFuncList;
    }


    //只拼接一个字段，预期异常
    public void concatCase079() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String concatSQL = "select name|| from " + strFuncTableName;
        ResultSet resultSet = statement.executeQuery(concatSQL);
        statement.close();
    }

    //concat函数拼接查询条件结果
    public List concatCase083() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String concatSQL = "select name||age||amount cnaa from " + strFuncTableName +
                " where (age<18 or age>60) and amount<100 and id >5";
        ResultSet resultSet = statement.executeQuery(concatSQL);
        List concatList = new ArrayList();
        while(resultSet.next()) {
            concatList.add(resultSet.getString("cnaa"));
        }

        statement.close();
        return concatList;
    }

    //concat函数拼接字符串
    public List concatCase084() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String concatSQL1 = "select 'Hello'||' World'||123 cons";
        String concatSQL2 = "select ''||''";
        String concatSQL3 = "select 0||1";
        String concatSQL4 = "select 'http:'||'//www'||'.baidu'||'.com'";
        String concatSQL5 = "select 'http'||'\\n'||'.baidu'||'\\t.com'";
        String concatSQL6 = "select 'abc'||'~!@#$%^&*()_+=-|][}{;:/.,?><'";
        String concatSQL7 = "select 'abc'||null";
        ResultSet resultSet1 = statement.executeQuery(concatSQL1);
        List concatList = new ArrayList();
        while(resultSet1.next()) {
            concatList.add(resultSet1.getString("cons"));
        }

        ResultSet resultSet2 = statement.executeQuery(concatSQL2);
        while(resultSet2.next()) {
            concatList.add(resultSet2.getString(1));
        }

        ResultSet resultSet3 = statement.executeQuery(concatSQL3);
        while(resultSet3.next()) {
            concatList.add(resultSet3.getString(1));
        }

        ResultSet resultSet4 = statement.executeQuery(concatSQL4);
        while(resultSet4.next()) {
            concatList.add(resultSet4.getString(1));
        }

        ResultSet resultSet5 = statement.executeQuery(concatSQL5);
        while(resultSet5.next()) {
            concatList.add(resultSet5.getString(1));
        }

        ResultSet resultSet6 = statement.executeQuery(concatSQL6);
        while(resultSet6.next()) {
            concatList.add(resultSet6.getString(1));
        }

        ResultSet resultSet7 = statement.executeQuery(concatSQL7);
        while(resultSet7.next()) {
            concatList.add(resultSet7.getString(1));
        }

        statement.close();
        return concatList;
    }

    //concat函数拼接字符串
    public List concatFuncUsingConcat() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String concatSQL1 = "select concat('Hello',' World')";
        String concatSQL2 = "select concat(concat('http://','www.'),'baidu.com')";
        ResultSet resultSet1 = statement.executeQuery(concatSQL1);
        List concatList = new ArrayList();
        while(resultSet1.next()) {
            concatList.add(resultSet1.getString(1));
        }

        ResultSet resultSet2 = statement.executeQuery(concatSQL2);
        while(resultSet2.next()) {
            concatList.add(resultSet2.getString(1));
        }

        statement.close();
        return concatList;
    }


    //拼接不存在的字段，预期异常
    public void concatCase085() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String concatSQL = "select name||age||birthday from " + strFuncTableName + " where id=1";
        ResultSet resultSet = statement.executeQuery(concatSQL);
        statement.close();
    }

    //格式化整型字段，保留大于0位小数位
    public List formatCase087() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(age,2) fage from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        List formatList = new ArrayList();
        while (formatRst.next()){
            formatList.add(formatRst.getString("fage"));
        }
        statement.close();
        return formatList;
    }

    //格式化整型字段，保留0位小数位
    public List formatCase088() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(age,0) fage from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        List formatList = new ArrayList();
        while (formatRst.next()){
            formatList.add(formatRst.getString("fage"));
        }
        statement.close();
        return formatList;
    }

    //格式化字符型字段，预期异常
    public void formatCase089() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(name,2) fname from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        statement.close();
    }

    //格式化不存在的字段，预期异常
    public void formatCase090() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(birthday,2) fname from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        statement.close();
    }

    //格式化浮点型字段，保留0位小数位
    public List formatCase091() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(amount,0) from " + strFuncTableName;
        ResultSet formatRst = statement.executeQuery(formatSQL);
        List formatList = new ArrayList();
        while (formatRst.next()){
            formatList.add(formatRst.getString(1));
        }
        statement.close();
        return formatList;
    }

    //格式化浮点型字段，保留0位小数位
    public String formatCase092(String formatValue, String decimalNum) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String formatSQL = "select format(" + formatValue + "," + decimalNum +")";
        ResultSet formatRst = statement.executeQuery(formatSQL);
        String formatResultStr = null;
        while (formatRst.next()){
            formatResultStr = formatRst.getString(1);
        }
        statement.close();
        return formatResultStr;
    }

    //locate函数，参数均为字符串
    public String locateCase096(String subStr, String locateStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate('" + subStr + "','" + locateStr +"')";
        ResultSet locateRst = statement.executeQuery(locateSQL);
        String locateResultStr = null;
        while (locateRst.next()){
            locateResultStr = locateRst.getString(1);
        }
        statement.close();
        return locateResultStr;
    }

    //locate函数，子串为整型
    public String locateCase098(String subStr, String locateStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate(" + subStr + ",'" + locateStr +"')";
        ResultSet locateRst = statement.executeQuery(locateSQL);
        String locateResultStr = null;
        while (locateRst.next()){
            locateResultStr = locateRst.getString(1);
        }
        statement.close();
        return locateResultStr;
    }

    //locate函数，子串和父串均为整型
    public String locateCase099(String subStr, String locateStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate(" + subStr + "," + locateStr +")";
        ResultSet locateRst = statement.executeQuery(locateSQL);
        String locateResultStr = null;
        while (locateRst.next()){
            locateResultStr = locateRst.getString(1);
        }
        statement.close();
        return locateResultStr;
    }

    //locate函数，子串为字符，父串为整型
    public String locateCase101(String subStr, String locateStr) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate('" + subStr + "'," + locateStr +")";
        ResultSet locateRst = statement.executeQuery(locateSQL);
        String locateResultStr = null;
        while (locateRst.next()){
            locateResultStr = locateRst.getString(1);
        }
        statement.close();
        return locateResultStr;
    }

    //locate函数，验证不支持position用法
    public void locateCase112() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String locateSQL = "select locate('a','bcadmjsac',5)";
        ResultSet locateRst = statement.executeQuery(locateSQL);
        statement.close();
    }

    public void createTableCase113() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String createTableSQL = "create table " + "tableStrCase113" + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "primary key(id)"
                + ")";
        statement.execute(createTableSQL);

        String insertSQL = "insert into tableStrCase113 values " +
                "(1,'zhangsan',18,90.33,'beijing'),\n" +
                "(2,'lisi',35,120.98,'haidian'),\n" +
                "(3,'Hello',35,18.0,'beijing'),\n" +
                "(4,'HELLO2',15,23.0,'chaoyangdis_1 NO.street'),\n" +
                "(5,'lala',18,12.1234560987,'beijing'),\n" +
                "(6,'88',18,12.0,'changping 89'),\n" +
                "(7,'baba',99,23.51648,'haidian2'),\n" +
                "(8,'zala',100,54.0,'wuwuxi '),\n" +
                "(9,' uzlia ',28,23.6,'  maya'),\n" +
                "(10,'  MaiTeng',66,70.3,'ding TAO  '),\n" +
                "(11,'',0,0.01,'')";
        statement.execute(insertSQL);
        statement.close();
    }

    //验证locate函数在条件语句中使用
    public List locateCase113() throws SQLException, ClassNotFoundException {
        createTableCase113();
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String updateSql = "update tableStrCase113 set address='shanghai' where locate('beijing',address)=0";
        statement.executeUpdate(updateSql);
        String querySql = "select concat(id,address) from tableStrCase113";
        ResultSet resultSet = statement.executeQuery(querySql);
        List locateResultList = new ArrayList();

        while(resultSet.next()) {
            locateResultList.add(resultSet.getString(1));
        }

        statement.close();
        return locateResultList;
    }

    //验证lower函数直接使用
    public String lowerCase119(String inputCase) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String lowerSQL = "select lower('" + inputCase + "')";
        ResultSet lowerRst = statement.executeQuery(lowerSQL);
        String lowerResultStr = null;
        while (lowerRst.next()){
            lowerResultStr = lowerRst.getString(1);
        }
        statement.close();
        return lowerResultStr;
    }

    //验证upper函数直接使用
    public String upperCase125(String inputCase) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String upperSQL = "select upper('" + inputCase + "')";
        ResultSet upperRst = statement.executeQuery(upperSQL);
        String upperResultStr = null;
        while (upperRst.next()){
            upperResultStr = upperRst.getString(1);
        }
        statement.close();
        return upperResultStr;
    }

    //验证lower和upper函数查询起别名
    public String lowerUpperCase126() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String lcaseSQL = "select lcase('ABC1') as lc";
        ResultSet lcaseRst = statement.executeQuery(lcaseSQL);
        String luResultStr = null;
        while (lcaseRst.next()){
            luResultStr = lcaseRst.getString("lc");
        }

        String ucaseSQL = "select ucase('def2') as uc";
        ResultSet ucaseRst = statement.executeQuery(ucaseSQL);
        while (ucaseRst.next()){
            luResultStr += ucaseRst.getString("uc");
        }

        statement.close();
        return luResultStr;
    }

    //left函数，截取字符串
    public String leftCase127(String inputStr, String leftLength) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left('" + inputStr + "'," + leftLength +")";
        ResultSet resultSet = statement.executeQuery(leftSQL);
        String leftResultStr = null;
        while (resultSet.next()){
            leftResultStr = resultSet.getString(1);
        }
        statement.close();
        return leftResultStr;
    }

    //left函数，截取数字
    public String leftCase130(String inputNum, String leftLength) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left(" + inputNum + "," + leftLength +")";
        ResultSet resultSet = statement.executeQuery(leftSQL);
        String leftResultStr = null;
        while (resultSet.next()){
            leftResultStr = resultSet.getString(1);
        }
        statement.close();
        return leftResultStr;
    }

    //left函数，验证接收参数个数
    public void leftCase133_1() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left('SQL test123',‘abc’,2)";
        statement.executeQuery(leftSQL);
        statement.close();
    }

    //left函数，验证接收参数个数
    public void leftCase133_2() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left(‘abc’)";
        statement.executeQuery(leftSQL);
        statement.close();
    }

    //left函数，验证接收截取长度为字符
    public void leftCase134_1() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left('SQL test123',‘a’)";
        statement.executeQuery(leftSQL);
        statement.close();
    }

    //left函数在表格中使用
    public List<List> leftCase135() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String leftSQL = "select left(name,3) lname, left(address,10) laddr, left(age,1) lage, left(amount,4) lamo from "
                + strFuncTableName + " where id in (1,10,15)";
        ResultSet leftRst = statement.executeQuery(leftSQL);
        List<List> leftList = new ArrayList<List>();
        while (leftRst.next()){
            List rowList = new ArrayList();
            rowList.add(leftRst.getString("lname"));
            rowList.add(leftRst.getString("laddr"));
            rowList.add(leftRst.getString("lage"));
            rowList.add(leftRst.getString("lamo"));
            leftList.add(rowList);
        }
        statement.close();
        return leftList;
    }

    //right函数，截取字符串
    public String rightCase138(String inputStr, String rightLength) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right('" + inputStr + "'," + rightLength +")";
        ResultSet resultSet = statement.executeQuery(rightSQL);
        String rightResultStr = null;
        while (resultSet.next()){
            rightResultStr = resultSet.getString(1);
        }
        statement.close();
        return rightResultStr;
    }

    //right函数，截取数字
    public String rightCase141(String inputStr, String rightLength) throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right(" + inputStr + "," + rightLength +")";
        ResultSet resultSet = statement.executeQuery(rightSQL);
        String rightResultStr = null;
        while (resultSet.next()){
            rightResultStr = resultSet.getString(1);
        }
        statement.close();
        return rightResultStr;
    }

    //right函数，验证接收参数个数
    public void rightCase144_1() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right('SQL test123',‘abc’,2)";
        statement.executeQuery(rightSQL);
        statement.close();
    }

    //right函数，验证接收参数个数
    public void rightCase144_2() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right(‘abc’)";
        statement.executeQuery(rightSQL);
        statement.close();
    }

    //right函数，验证接收截取长度为字符
    public void rightCase145() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right('SQL test123',‘a’)";
        statement.executeQuery(rightSQL);
        statement.close();
    }

    //right函数在表格中使用
    public List<List> rightCase146() throws SQLException, ClassNotFoundException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String rightSQL = "select right(address,10) raddr, right(amount,4) ramo, right(age,2) rage from "
                + strFuncTableName + " where id in (1,2,15)";
        ResultSet rightRst = statement.executeQuery(rightSQL);
        List<List> rightList = new ArrayList<List>();
        while (rightRst.next()){
            List rowList = new ArrayList();
            rowList.add(rightRst.getString("raddr"));
            rowList.add(rightRst.getString("ramo"));
            rowList.add(rightRst.getString("rage"));
            rightList.add(rowList);
        }
        statement.close();
        return rightList;
    }

    //验证left和right函数和拼接函数一起使用
    public List leftRightCase149() throws SQLException, ClassNotFoundException {
        connection = connectStrDB();
        Statement statement = connection.createStatement();
        String querySQL1 = "select 'Hello'||left(' 1',1)||'World'";
        String querySQL2 = "select 'test'||left(' Dingotest',6)";
        String querySQL3 = "select 'Hello'||right('-+',1)||'World'";
        String querySQL4 = "select 'test-'||right(' Dingo',2)";
        ResultSet resultSet1 = statement.executeQuery(querySQL1);
        List concatList = new ArrayList();
        while(resultSet1.next()) {
            concatList.add(resultSet1.getString(1));
        }

        ResultSet resultSet2 = statement.executeQuery(querySQL2);
        while(resultSet2.next()) {
            concatList.add(resultSet2.getString(1));
        }

        ResultSet resultSet3 = statement.executeQuery(querySQL3);
        while(resultSet3.next()) {
            concatList.add(resultSet3.getString(1));
        }

        ResultSet resultSet4 = statement.executeQuery(querySQL4);
        while(resultSet4.next()) {
            concatList.add(resultSet4.getString(1));
        }

        statement.close();
        return concatList;
    }



}
