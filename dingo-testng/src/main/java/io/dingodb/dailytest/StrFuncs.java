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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class StrFuncs {
    private static final String defaultConnectIP = "172.20.3.26";
//    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
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
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateNowStr = simpleDateFormat.format(date);
        String tableName = funcTablePrefix + dateNowStr;
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

}
