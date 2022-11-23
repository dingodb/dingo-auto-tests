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
    public static String tableName1 = "lktest1";

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
    public void test00TableCreate(Map<String, String> param) throws SQLException {
        String tableMetaPath = param.get("metaPath");
        String tableValuePath = param.get("valuePath");
        initTable(param.get("tableName"), tableMetaPath);
        insertTableValue(param.get("tableName"), param.get("insertFields"), tableValuePath);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00TableCreate"}, dataProvider = "likeStateMethod",
            description = "验证like查询%，_和[]，返回数据")
    public void test01LikeQueryData(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedList = strTo2DList.construct2DList(param.get("queryRst"),";",",");
        System.out.println("Expected: " + expectedList);
        List<List> actualList = likeObj.likeQueryList(param.get("tableName"), param.get("queryFields"), param.get("queryState"),9, param.get("outFields"));
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
