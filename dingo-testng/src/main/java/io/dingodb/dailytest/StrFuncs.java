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
        //connection.close();
    }

    // 插入数据
    public int insertValues() throws ClassNotFoundException, SQLException {
        String strFuncTableName = getStrTableName();
        connection = connectStrDB();
        Statement statement = connection.createStatement();

        String batInsertSql = "insert into " + strFuncTableName +
                " values (1,'zhangsan',18,23.50,'beijing'),\n" +
                "(2,'lisi',25,89,' beijing haidian '),\n" +
                "(3,'lisi3',55,123.123,'wuhan NO.1 Street'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(5,'wJDs',1,1453.9999,'pingYang1'),\n" +
                "(6,'123',544,0,'543'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang'),\n" +
                "(8,'zhangsan',18,12.3,'shanghai'),\n" +
                "(9,'oppo',76,109.325,'wuhan'),\n" +
                "(10,'lisi',256,1234.456,'nanjing'),\n" +
                "(11,'  ab c  d ',61,99.9999,'beijing chaoyang'),\n" +
                "(12,' abcdef',2,2345.000,'123'),\n" +
                "(13,'HAHA',57,9.0762556,'CHANGping'),\n" +
                "(14,'zhangsan',99,32,'chong qing '),\n" +
                "(15,'1.5',18,0.1235,'hebei')";
        int insertRows = statement.executeUpdate(batInsertSql);
        statement.close();
//        connection.close();
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
//        connection.close();
        return concatStr;
    }



}
