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

import io.dingodb.dailytest.TableCreate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestTableCreate {
    public static TableCreate tableCreateObj = new TableCreate();

    public void initTable1() throws SQLException {
        String table1_meta_path = "src/test/resources/testdata/tablemeta/createTableTest/meta/createtable1_meta.txt";
        String table1_value_path = "src/test/resources/testdata/tablemeta/createTableTest/values/createtable1_values.txt";
        String table1_meta = FileReaderUtil.readFile(table1_meta_path);
        String table1_value = FileReaderUtil.readFile(table1_value_path);
        tableCreateObj.createTableWithDefaultValue(table1_meta);
        tableCreateObj.insertTable1Values(table1_value);
    }

    public static List<List> expectedTable1List() {
        String[][] dataArray = {
                {"1","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"},
                {"2","zhangsan","18","BJ Road.1","1234.5678","1991-06-18","23:59:59","2022-08-12 14:00:00","false"}
        };
        List<List> tableList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            tableList.add(columnList);
        }
        return tableList;
    }

    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(TableCreate.connection);
    }

    @Test(enabled = true, description = "创建测试表1,所有字段均不允许为null，所有字段均有默认值，只插入主键")
    public void test01CreateTable1() throws SQLException {
        initTable1();
    }
    @Test(enabled = true, description = "验证table1表数据")
    public void test01Table1Records() throws SQLException {
        List<List> expectedRecords = expectedTable1List();
        System.out.println("Expected: " + expectedRecords);

        List<List> actualRecords = tableCreateObj.queryTable1Data();
        System.out.println("Actual: " + expectedRecords);
        Assert.assertEquals(actualRecords, expectedRecords);
    }



}
