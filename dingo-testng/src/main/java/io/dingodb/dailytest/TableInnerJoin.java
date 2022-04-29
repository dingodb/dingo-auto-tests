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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TableInnerJoin {
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

    public void createInnerTables() throws SQLException {
        String innerTable1 = "beauty";
        String innerTable2 = "boys";
        Statement statement = connection.createStatement();
        String createSQL1 = "create table " + innerTable1 + "(id int NOT NULL,\n" +
                "name varchar(50) NOT NULL,\n" +
                "sex varchar(10) DEFAULT 'female',\n" +
                "borndate timestamp DEFAULT '1987-01-01 00:00:00',\n" +
                "phone varchar(11) NOT NULL,\n" +
                "boyfriend_id int DEFAULT NULL,\n" +
                "PRIMARY KEY (id)\n" +
                ")";

        String createSQL2 = "create table " + innerTable2 + "(\n" +
                "id int NOT NULL,\n" +
                "boyName varchar(20) DEFAULT NULL,\n" +
                "userCP int DEFAULT NULL,\n" +
                "PRIMARY KEY (id)\n" +
                ")";
        statement.execute(createSQL1);
        statement.execute(createSQL2);
        statement.close();
    }

    public void insertDataToInnerTables() throws SQLException {
        Statement statement = connection.createStatement();
        String table1Values = "(1,'LiuYan','female','1988-02-03 00:00:00','18209876577',8),\n" +
                "(2,'TeacherLi','female','1987-12-30 00:00:00','18219876577',9),\n" +
                "(3,'Angelay','female','1989-02-03 00:00:00','18209876567',3),\n" +
                "(4,'ReBa','female','1993-02-03 00:00:00','18209876579',2),\n" +
                "(5,'DuLala','female','1992-02-03 00:00:00','18209179577',9),\n" +
                "(6,'zhiRuo','female','1988-02-03 00:00:00','18209876577',1),\n" +
                "(7,'LingShan','female','1987-12-30 00:00:00','18219876577',9),\n" +
                "(8,'Xiao Zhao','female','1989-02-03 00:00:00','18209876567',1),\n" +
                "(9,'Shuange','female','1993-02-03 00:00:00','18209876579',9),\n" +
                "(10,'Wang Yuyan','female','1992-02-03 00:00:00','18209179577',4),\n" +
                "(11,'Xia Xue','female','1993-02-03 00:00:00','18209876579',9),\n" +
                "(12,'Zhao Min','female','1992-02-03 00:00:00','18209179577',1)";

        String table2Values = "(1,'Zhang Wuji',100),(2,'Han Han',800),(3,'Xiao Ming',50),(4,'DuanYU',300)";

        String insertSQL1 = "insert into beauty values " + table1Values;
        String insertSQL2 = "insert into boys values " + table2Values;

        statement.execute(insertSQL1);
        statement.execute(insertSQL2);
        statement.close();
    }

    public List<List<String>> innerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        Statement statement = connection.createStatement();
        String joinSQL = "select name, boyname from beauty inner join boys on beauty.boyfriend_id = boys.id";
        ResultSet joinRst = statement.executeQuery(joinSQL);
        List<List<String>> joinList = new ArrayList<List<String>>();
        while (joinRst.next()) {
            List<String> equalRowList = new ArrayList<String>();
            equalRowList.add(joinRst.getString(1));
            equalRowList.add(joinRst.getString(2));
            joinList.add(equalRowList);
        }
        return joinList;
    }

    public List<List<String>> innerJoinWithTableAlias () throws SQLException {
        Statement statement = connection.createStatement();
        String joinAliasSQL = "select g.name, b.boyname from beauty as g inner join boys as b on g.boyfriend_id = b.id";
        ResultSet joinAliasRst = statement.executeQuery(joinAliasSQL);
        List<List<String>> joinAliasList = new ArrayList<List<String>>();
        while (joinAliasRst.next()) {
            List<String> equalRowAliasList = new ArrayList<String>();
            equalRowAliasList.add(joinAliasRst.getString(1));
            equalRowAliasList.add(joinAliasRst.getString(2));
            joinAliasList.add(equalRowAliasList);
        }
        return joinAliasList;
    }

}
