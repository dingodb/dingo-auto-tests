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
import io.dingodb.dailytest.PartitionParam;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class TestPartitionParam extends YamlDataHelper {
    public static PartitionParam partitionObj = new PartitionParam();

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        partitionObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        partitionObj.insertTableValues(tableName, insertFields, tableValues);
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(partitionObj.connection);
    }

    @Test(priority = 0, enabled = true, dataProvider = "partitionParamMethod", description = "验证分区表创建成功")
    public void test01PartitionTableCreatePos(Map<String, String> param) throws SQLException {
        String tableMetaPath = param.get("metaPath");
        initTable(param.get("tableName"), tableMetaPath);
    }

    @Test(priority = 1, enabled = true, dataProvider = "partitionParamMethod", expectedExceptions = SQLException.class, description = "验证分区参数错误，创建表失败")
    public void test02PartitionTableCreateNeg(Map<String, String> param) throws SQLException {
        String tableMetaPath = param.get("metaPath");
        initTable(param.get("tableName"), tableMetaPath);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();
        try{
            tearDownStatement = partitionObj.connection.createStatement();
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
            JDBCUtils.closeResource(partitionObj.connection, tearDownStatement);
        }
    }

}
