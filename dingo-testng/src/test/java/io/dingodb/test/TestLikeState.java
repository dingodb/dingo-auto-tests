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

import io.dingodb.common.utils.JDBCUtils;
import io.dingodb.dailytest.LikeState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.StrTo2DList;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestLikeState extends YamlDataHelper {
    public static LikeState likeObj = new LikeState();
    public static String tableName4 = "lktest4";

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        likeObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        likeObj.insertTableValues(tableName, insertFields, tableValues);
    }

    public static List<List> expectedOutput(String[][] dataArray) {
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(likeObj.connection);
    }

    @Test(priority = 0, enabled = true, dataProvider = "likeStateMethod", description = "创建表并插入数据")
    public void test00TableCreate(Map<String, String> param) throws SQLException, InterruptedException {
        String tableMetaPath = param.get("metaPath");
        String tableValuePath = param.get("valuePath");
        initTable(param.get("tableName"), tableMetaPath);
        Thread.sleep(5000);
        insertTableValue(param.get("tableName"), param.get("insertFields"), tableValuePath);
    }

    @Test(priority = 1, enabled = true, dataProvider = "likeStateMethod",
            description = "验证like查询%，_和[]，返回数据")
    public void test01LikeQueryData(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DListIncludeBlank(param.get("queryRst"),";",",");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = likeObj.likeQueryByOutFields(param.get("tableName"), param.get("queryFields"), param.get("queryState"), param.get("outFields"));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 2, enabled = true, dataProvider = "likeStateMethod", description = "验证like查询%，_和[]，返回行数")
    public void test02LikeQueryRows(Map<String, String> param) throws SQLException {
        int expectedRows = Integer.parseInt(param.get("queryRst"));
        System.out.println("Expected: " + expectedRows);
        int actualRows = likeObj.likeQueryCount(param.get("tableName"), param.get("queryFields"), param.get("queryState"));
        System.out.println("Actual: " + actualRows);

        Assert.assertEquals(actualRows, expectedRows);
    }

    @Test(priority = 3, enabled = true, dataProvider = "likeStateMethod", expectedExceptions = SQLException.class,
            description = "验证like查询预期失败场景")
    public void test03LikeQueryFail(Map<String, String> param) throws SQLException {
        likeObj.likeQueryFail(param.get("tableName"), param.get("queryFields"), param.get("queryState"));
    }

    @Test(priority = 4, enabled = true, description = "like语句模糊匹配字段不在查询字段里")
    public void test04LikeFieldNotInQuery() throws SQLException {
        String[][] dataArray = {
                {"2", "45", "25.46", "1999-04-06"},{"5", "90", "138.05", "2010-10-31"},
                {"11", "100", null, "2008-10-30"}, {"19", "88", "20010.1234", "1942-01-01"},
                {"21", "88", "797.0", "1942-01-01"},{"22", "59", "20015.1234", "2011-12-31"}
        };
        List<List> expectedList = expectedOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "id,age,amount,birthday";
        String queryState = "where name like 'li%'";
        List<List> actualList = likeObj.likeQueryByQueryFields(tableName4, queryFields, queryState, 9);
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 5, enabled = true, description = "like语句在全连接中使用")
    public void test05LikeInFullJoin() throws SQLException {
        String[][] dataArray = {
                {"3", "3"},{"5", null}, {"9", null}, {"6", "1"}, {"8", "1"},{"4", "2"}
        };
        List<List> expectedList = expectedOutput(dataArray);
        System.out.println("Expected: " + expectedList);
        String queryFields = "lkbeauty.id,lkboys.id";
        String tableName = "lkbeauty full outer join lkboys on lkbeauty.boyfriend_id=lkboys.id";
        String queryState = "where lkbeauty.name not like '%li%' and lkbeauty.id<10";
        List<List> actualList = likeObj.likeQueryByQueryFields(tableName, queryFields, queryState, 9);
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 6, enabled = true, dataProvider = "likeStateMethod", description = "验证更新语句中使用Like子句")
    public void test06LikeInUpdateState(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DListIncludeBlank(param.get("queryRst"),";",",");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = likeObj.likeInUpdateState(param.get("updateSql"), param.get("tableName"), param.get("queryFields"), param.get("queryState"), param.get("outFields"));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }

    @Test(priority = 7, enabled = true, dataProvider = "likeStateMethod", description = "验证删除语句中使用Like子句")
    public void test07LikeInDeleteState(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DListIncludeBlank(param.get("queryRst"),";",",");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = likeObj.likeInDeleteState(param.get("deleteSql"), param.get("tableName"), param.get("queryFields"), param.get("queryState"), param.get("outFields"));
        System.out.println("Actual: " + actualList);

        Assert.assertTrue(actualList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();
        try{
            tearDownStatement = likeObj.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(likeObj.connection, tearDownStatement);
        }
    }

}
