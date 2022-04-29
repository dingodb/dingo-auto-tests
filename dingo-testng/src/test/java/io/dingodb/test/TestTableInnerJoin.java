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

import io.dingodb.dailytest.TableInnerJoin;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestTableInnerJoin {
    public static TableInnerJoin tableJoinObj = new TableInnerJoin();

    public static List<List<String>> girlsJoinList() {
        String[][] beautyBoyArray = {{"ReBa","Han Han"},{"zhiRuo","Zhang Wuji"}, {"Xiao Zhao", "Zhang Wuji"},
                {"Wang Yuyan", "DuanYU"}, {"Zhao Min", "Zhang Wuji"},{"Angelay", "Xiao Ming"}};
        List<List<String>> girlsList = new ArrayList<List<String>>();
        for(int i=0; i<beautyBoyArray.length; i++) {
            List<String> columnList = new ArrayList<>();
            for (int j=0; j<beautyBoyArray[i].length; j++) {
                columnList.add(beautyBoyArray[i][j]);
            }
            girlsList.add(columnList);
        }
        return girlsList;
    }

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库，创建表格和插入数据")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(TableInnerJoin.connection);
        tableJoinObj.createInnerTables();
        tableJoinObj.insertDataToInnerTables();
    }

    @Test(priority = 0, enabled = true, description = "验证等值连接查询各自特有字段不使用表名修饰")
    public void test01InnerJoinOwnFieldWithoutTablePrefix() throws SQLException {
        List<List<String>> expectedInnerJoinList = girlsJoinList();
        System.out.println("Expected: " + expectedInnerJoinList);
        List<List<String>> actualInnerJoinList = tableJoinObj.innerJoinOwnFieldWithoutTablePrefix();
        System.out.println("Acutal: " + actualInnerJoinList);

        for(int i=0; i<expectedInnerJoinList.size(); i++) {
            Assert.assertTrue(actualInnerJoinList.contains(expectedInnerJoinList.get(i)));
        }
        for(int j=0; j<actualInnerJoinList.size(); j++) {
            Assert.assertTrue(expectedInnerJoinList.contains(actualInnerJoinList.get(j)));
        }
    }

    @Test(priority = 1, enabled = true, description = "验证等值连接使用别名")
    public void test02InnerJoinWithTableAlias() throws SQLException {
        List<List<String>> expectedInnerJoinWithTableAliasList = girlsJoinList();
        System.out.println("Expected: " + expectedInnerJoinWithTableAliasList);
        List<List<String>> actualInnerJoinWithTableAliasList = tableJoinObj.innerJoinWithTableAlias();
        System.out.println("Acutal: " + actualInnerJoinWithTableAliasList);

        for(int i=0; i<expectedInnerJoinWithTableAliasList.size(); i++) {
            Assert.assertTrue(actualInnerJoinWithTableAliasList.contains(expectedInnerJoinWithTableAliasList.get(i)));
        }
        for(int j=0; j<actualInnerJoinWithTableAliasList.size(); j++) {
            Assert.assertTrue(expectedInnerJoinWithTableAliasList.contains(actualInnerJoinWithTableAliasList.get(j)));
        }
    }

    @Test(priority = 1, enabled = true, expectedExceptions = SQLException.class, description = "验证缺少等值条件，预期异常")
    public void test03InnerJoinWithoutCondition() throws SQLException {
        Statement noConditionStatement = TableInnerJoin.connection.createStatement();
        String innerJoinWithoutConditionSQL = "select name, boyname from beauty inner join boys";
        noConditionStatement.executeQuery(innerJoinWithoutConditionSQL);
        noConditionStatement.close();
    }

    @Test(priority = 1, enabled = true, expectedExceptions = SQLException.class, description = "验证起别名后等值条件依然使用表原名，预期异常")
    public void test04InnerJoinAliasWithOriginalName() throws SQLException {
        Statement originalStatement = TableInnerJoin.connection.createStatement();
        String innerJoinAliasWithOriginalNameSQL = "select g.id, g.name, b.boyname from beauty as g inner join boys as b on beauty.boyfriend_id = boys.id";
        originalStatement.executeQuery(innerJoinAliasWithOriginalNameSQL);
        originalStatement.close();
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = TableInnerJoin.connection.createStatement();
        tearDownStatement.execute("delete from beauty");
        tearDownStatement.execute("drop table beauty");
        tearDownStatement.execute("delete from boys");
        tearDownStatement.execute("drop table boys");

        tearDownStatement.close();
        TableInnerJoin.connection.close();
    }
}
