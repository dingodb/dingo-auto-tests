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

public class NumericFuncs {
//    private static final String defaultConnectIP = "172.20.3.27";
//    private static final String defaultConnectIP = "172.20.61.1";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";
    public static Connection connection = null;

    static{
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(connectUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //pow函数，正向参数用例
    public String powPositiveArg(String num1, String num2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String powSQL = "select pow(" + num1 + "," + num2 +")";
            ResultSet resultSet = statement.executeQuery(powSQL);
            String powStr = null;
            while (resultSet.next()){
                powStr = resultSet.getString(1);
            }
            statement.close();
            return powStr;
        }
    }

    //pow函数，x参数为字符串，预期失败
    public void powxStr(String num1, String num2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String powSQL = "select pow('" + num1 + "'," + num2 +")";
            statement.executeQuery(powSQL);
        }
    }

    //pow函数，y参数为字符串，预期失败
    public void powyStr(String num1, String num2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String powSQL = "select pow(" + num1 + ",'" + num2 +"')";
            statement.executeQuery(powSQL);
        }
    }

    //pow函数，参数不合法，预期失败
    public void powWrongArg(String powState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.executeQuery(powState);
        }
    }

    //round函数，正向参数用例
    public String roundPositiveArg(String inputNum, String decimalLen) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String roundSQL = "select round(" + inputNum + "," + decimalLen +")";
            ResultSet resultSet = statement.executeQuery(roundSQL);
            String roundStr = null;
            while (resultSet.next()){
                roundStr = resultSet.getString(1);
            }
            statement.close();
            return roundStr;
        }
    }

    //round函数，x为字符串，y为整数，预期失败
    public void roundxStr(String inputNum, String decimalLen) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String roundSQL = "select round('" + inputNum + "'," + decimalLen +")";
            statement.executeQuery(roundSQL);
        }
    }

    //round函数，x为小数，y为字符串，预期失败
    public void roundyStr(String inputNum, String decimalLen) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String roundSQL = "select round(" + inputNum + ",'" + decimalLen +"')";
            statement.executeQuery(roundSQL);
        }
    }

    //round函数，xy均为字符串，预期失败
    public void roundxyStr(String inputNum, String decimalLen) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String roundSQL = "select round('" + inputNum + "','" + decimalLen +"')";
            statement.executeQuery(roundSQL);
        }
    }

    //round函数，参数不合法，预期失败
    public void roundWrongArg(String roundState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.executeQuery(roundState);
        }
    }

    //round函数，只有x参数，按默认0位小数处理
    public String roundXOnly(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String roundSQL = "select round(" + inputNum + ")";
            ResultSet resultSet = statement.executeQuery(roundSQL);
            String roundStr = null;
            while (resultSet.next()){
                roundStr = resultSet.getString(1);
            }
            statement.close();
            return roundStr;
        }
    }

    //ceiling函数，正向用例
    public String ceilingPositiveArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ceilingSQL = "select ceiling(" + inputNum + ")";
            ResultSet resultSet = statement.executeQuery(ceilingSQL);
            String ceilingStr = null;
            while (resultSet.next()){
                ceilingStr = resultSet.getString(1);
            }
            statement.close();
            return ceilingStr;
        }
    }

    //ceiling函数，参数个数不符，预期失败
    public void ceilingWrongArg(String ceilingState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.executeQuery(ceilingState);
        }
    }

    //ceiling函数，参数为字符串，预期失败
    public void ceilingStrArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ceilingSQL = "select ceiling('" + inputNum + "')";
            statement.executeQuery(ceilingSQL);
        }
    }

    //ceil函数，同ceiling函数
    public String ceilFunc(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String ceilSQL = "select ceil(" + inputNum + ")";
            ResultSet resultSet = statement.executeQuery(ceilSQL);
            String ceilStr = null;
            while (resultSet.next()){
                ceilStr = resultSet.getString(1);
            }
            statement.close();
            return ceilStr;
        }
    }

    //floor函数，正向用例
    public String floorPositiveArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String floorSQL = "select floor(" + inputNum + ")";
            ResultSet resultSet = statement.executeQuery(floorSQL);
            String floorStr = null;
            while (resultSet.next()){
                floorStr = resultSet.getString(1);
            }
            statement.close();
            return floorStr;
        }
    }

    //floor函数，参数个数不符，预期失败
    public void floorWrongArg(String floorState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.executeQuery(floorState);
        }
    }

    //floor函数，参数为字符串，预期失败
    public void floorStrArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String floorSQL = "select floor('" + inputNum + "')";
            statement.executeQuery(floorSQL);
        }
    }

    //abs函数，正向用例
    public String absPositiveArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String absSQL = "select floor(" + inputNum + ")";
            ResultSet resultSet = statement.executeQuery(absSQL);
            String absStr = null;
            while (resultSet.next()){
                absStr = resultSet.getString(1);
            }
            statement.close();
            return absStr;
        }
    }

    //abs函数，参数个数不符，预期失败
    public void absWrongArg(String absState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.executeQuery(absState);
        }
    }

    //abs函数，参数为字符串，预期失败
    public void absStrArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String absSQL = "select abs('" + inputNum + "')";
            statement.executeQuery(absSQL);
        }
    }






}
