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

import io.dingodb.dailytest.TableOuterJoin;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestTableOuterJoin {
    public static TableOuterJoin outerJoinObj = new TableOuterJoin();

    public void initGirlsTB() throws SQLException {
        String beauty_tbl_meta_path = "src/test/resources/testdata/tablemeta/beauty_tbl_meta.txt";
        String beauty_tbl_value_path = "src/test/resources/testdata/tableInsertValues/beauty_tbl.txt";
        String beauty_tbl_meta = FileReaderUtil.readFile(beauty_tbl_meta_path);
        String beauty_tbl_value = FileReaderUtil.readFile(beauty_tbl_value_path);
        outerJoinObj.createBeautyTable(beauty_tbl_meta);
        outerJoinObj.insertValuesToBeauty(beauty_tbl_value);
        String boys_tbl_meta_path = "src/test/resources/testdata/tablemeta/boys_tbl_meta.txt";
        String boys_tbl_value_path = "src/test/resources/testdata/tableInsertValues/boys_tbl.txt";
        String boys_right_value_path = "src/test/resources/testdata/tableInsertValues/boys_right.txt";
        String boys_tbl_meta = FileReaderUtil.readFile(boys_tbl_meta_path);
        String boys_tbl_value = FileReaderUtil.readFile(boys_tbl_value_path);
        String boys_right_value = FileReaderUtil.readFile(boys_right_value_path);
        outerJoinObj.createBoysTable(boys_tbl_meta);
        outerJoinObj.insertValuesToBoys(boys_tbl_value);
        outerJoinObj.createBoysRightTable(boys_tbl_meta);
        outerJoinObj.insertValuesToBoysRight(boys_right_value);
    }

    public void initEmployeesTB() throws SQLException {
        String departments_tbl_meta_path = "src/test/resources/testdata/tablemeta/departments_tbl_meta.txt";
        String departments_tbl_value_path = "src/test/resources/testdata/tableInsertValues/departments.txt";
        String departments_tbl_meta = FileReaderUtil.readFile(departments_tbl_meta_path);
        String departments_tbl_value = FileReaderUtil.readFile(departments_tbl_value_path);
        outerJoinObj.createDepartmentsTable(departments_tbl_meta);
        outerJoinObj.insertValuesToDepartments(departments_tbl_value);
        String employees_tbl_meta_path = "src/test/resources/testdata/tablemeta/employees_tbl_meta.txt";
        String employeess_tbl_value_path = "src/test/resources/testdata/tableInsertValues/employees.txt";
        String employees_tbl_meta = FileReaderUtil.readFile(employees_tbl_meta_path);
        String employees_tbl_value = FileReaderUtil.readFile(employeess_tbl_value_path);
        outerJoinObj.createEmployeesTable(employees_tbl_meta);
        outerJoinObj.insertValuesToEmployees(employees_tbl_value);
    }

    public void initStudentAndClassTB() throws SQLException {
        String student_tbl_meta_path = "src/test/resources/testdata/tablemeta/student_tbl_meta.txt";
        String student_tbl_value_path = "src/test/resources/testdata/tableInsertValues/student_tbl.txt";
        String student_tbl_meta = FileReaderUtil.readFile(student_tbl_meta_path);
        String student_tbl_value = FileReaderUtil.readFile(student_tbl_value_path);
        outerJoinObj.createStudentTable(student_tbl_meta);
        outerJoinObj.insertValuesToStudent(student_tbl_value);
        outerJoinObj.createStudent1Table(student_tbl_meta);
        String class_tbl_meta_path = "src/test/resources/testdata/tablemeta/class_tbl_meta.txt";
        String class_tbl_value_path = "src/test/resources/testdata/tableInsertValues/class_tbl.txt";
        String class_tbl_meta = FileReaderUtil.readFile(class_tbl_meta_path);
        String class_tbl_value = FileReaderUtil.readFile(class_tbl_value_path);
        outerJoinObj.createClassTable(class_tbl_meta);
        outerJoinObj.insertValuesToClass(class_tbl_value);
        outerJoinObj.createClass1Table(class_tbl_meta);
    }

    public void initProductTB() throws SQLException {
        String product1_meta_path = "src/test/resources/testdata/tablemeta/product1_meta.txt";
        String product1_value_path = "src/test/resources/testdata/tableInsertValues/product1.txt";
        String product1_meta = FileReaderUtil.readFile(product1_meta_path);
        String product1_value = FileReaderUtil.readFile(product1_value_path);
        outerJoinObj.createProduct1Table(product1_meta);
        outerJoinObj.insertValuesToProduct1(product1_value);
        String product2_meta_path = "src/test/resources/testdata/tablemeta/product2_meta.txt";
        String product2_value_path = "src/test/resources/testdata/tableInsertValues/product2.txt";
        String product2_meta = FileReaderUtil.readFile(product2_meta_path);
        String product2_value = FileReaderUtil.readFile(product2_value_path);
        outerJoinObj.createProuct2Table(product2_meta);
        outerJoinObj.insertValuesToProduct2(product2_value);
        outerJoinObj.createProuct3Table(product2_meta);
    }

    public void initTestTB() throws SQLException {
        String test_meta_path = "src/test/resources/testdata/tablemeta/test_meta.txt";
        String test1_value_path = "src/test/resources/testdata/tableInsertValues/test1.txt";
        String test_meta = FileReaderUtil.readFile(test_meta_path);
        String test1_value = FileReaderUtil.readFile(test1_value_path);
        outerJoinObj.createTest1Table(test_meta);
        outerJoinObj.insertValuesToTest1(test1_value);
        String test2_value_path = "src/test/resources/testdata/tableInsertValues/test2.txt";
        String test2_value = FileReaderUtil.readFile(test2_value_path);
        outerJoinObj.createTest2Table(test_meta);
        outerJoinObj.insertValuesToTest2(test2_value);
    }

    public void initW3cTB() throws SQLException {
        String w3c_meta_path = "src/test/resources/testdata/tablemeta/w3cschool_tbl_meta.txt";
        String w3c_value_path = "src/test/resources/testdata/tableInsertValues/w3cschool_tbl.txt";
        String w3c_meta = FileReaderUtil.readFile(w3c_meta_path);
        String w3c_value = FileReaderUtil.readFile(w3c_value_path);
        outerJoinObj.createw3cTable(w3c_meta);
        outerJoinObj.insertValuesTow3c(w3c_value);
        String tcount_meta_path = "src/test/resources/testdata/tablemeta/tcount_tbl_meta.txt";
        String tcount_value_path = "src/test/resources/testdata/tableInsertValues/tcount_tbl.txt";
        String tcount_meta = FileReaderUtil.readFile(tcount_meta_path);
        String tcount_value = FileReaderUtil.readFile(tcount_value_path);
        outerJoinObj.createtcountTable(tcount_meta);
        outerJoinObj.insertValuesTotcount(tcount_value);
    }


    public List<List> expectedFullList1() {
        String[][] dataArray = {{"2","lisi","101","101","class-2"},{"1","zhangsan","100","100","class-1"},
                {null,null,null,"103","class-3"},{"3","wangwu","102",null,null}};
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

    public List<List> expectedFullList2() {
        String[][] dataArray = {{"1","2","3",null,null},{"4","5","6",null,null},
                {"7","8","9",null,null},{null,null,null,"3","1"},{null,null,null,"6","2"}};
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

    public List<List> expectedFullList3() {
        String[][] dataArray = {{"2","lisi","101",null,null},{"1","zhangsan","100",null,null},
                {"3","wangwu","102",null,null}};
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

    public List<List> expectedFullList4() {
        String[][] dataArray = {{"2","Lisi",null,null},{null,null,"5","Hello"},{"1","Zhangsan","1","Zhangsan"},
                {"4","Tita",null,null},{"3","Wang Wu","3","Wang Wu"},{null,null,"6","NiNi"}};
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

    public List<List> expectedFullList5() {
        String[][] dataArray = {{"2","lisi","class-2"},{"1","zhangsan","class-1"},
                {null,null,"class-3"},{"3","wangwu",null}};
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

    public List<List> expectedFullList6() {
        String[][] dataArray = {{"2","lisi","101","101","class-2"},{"1","zhangsan","100","100","class-1"}};
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

    public List<List> expectedFullList7() {
        String[][] dataArray = {{null,null,null,"103","class-3"},{"3","wangwu","102",null,null}};
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

    public List<List> expectedCrossList1() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"},
                {"1","zhangsan","100","101","class-2"}, {"1","zhangsan","100","103","class-3"},
                {"2","lisi","101","100","class-1"}, {"2","lisi","101","101","class-2"},
                {"2","lisi","101","103","class-3"}, {"3","wangwu","102","100","class-1"},
                {"3","wangwu","102","101","class-2"}, {"3","wangwu","102","103","class-3"}};
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

    public List<List> expectedCrossList2() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"}, {"2","lisi","101","101","class-2"}};
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

    public List<List> expectedCrossList3() {
        String[][] dataArray = {{"1","Zhangsan","1","Zhangsan"}, {"1","Zhangsan","3","Wang Wu"},
                {"1","Zhangsan","5","Hello"},{"1","Zhangsan","6","NiNi"},
                {"2","Lisi","1","Zhangsan"}, {"2","Lisi","3","Wang Wu"},
                {"2","Lisi","5","Hello"},{"2","Lisi","6","NiNi"},
                {"3","Wang Wu","1","Zhangsan"}, {"3","Wang Wu","3","Wang Wu"},
                {"3","Wang Wu","5","Hello"},{"3","Wang Wu","6","NiNi"},
                {"4","Tita","1","Zhangsan"}, {"4","Tita","3","Wang Wu"},
                {"4","Tita","5","Hello"},{"4","Tita","6","NiNi"}};
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

    public List<List> expectedCrossList4() {
        String[][] dataArray = {{"1","zhangsan","class-1"},
                {"1","zhangsan","class-2"}, {"1","zhangsan","class-3"},
                {"2","lisi","class-1"}, {"2","lisi","class-2"},
                {"2","lisi","class-3"}, {"3","wangwu","class-1"},
                {"3","wangwu","class-2"}, {"3","wangwu","class-3"}};
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

    public List<List> expectedCrossList5() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"},
                {"2","lisi","101","100","class-1"}, {"3","wangwu","102","100","class-1"},
                {"1","zhangsan","100","101","class-2"}, {"2","lisi","101","101","class-2"},
                {"3","wangwu","102","101","class-2"}, {"1","zhangsan","100","103","class-3"},
                {"2","lisi","101","103","class-3"}, {"3","wangwu","102","103","class-3"}};
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

    public List<List> expectedLeftList1() {
        String[][] dataArray = {{"LiuYan","8"}, {"TeacherLi","9"}, {"DuLala","9"},
                {"LingShan","9"}, {"Shuange","9"}, {"Xia Xue","9"}};
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

    public List<List> expectedLeftList2() {
        String[][] dataArray = {{"1","LiuYan","female","1988-02-03 00:00:00","18209876577","8",null,null,null},
                {"2","TeacherLi","female","1987-12-30 00:00:00","18219876577","9",null,null,null},
                {"3","Angelay","female","1989-02-03 00:00:00","18209876567","3","3","Xiao Ming","50"},
                {"4","ReBa","female","1993-02-03 00:00:00","18209876579","2","2","Han Han","800"},
                {"5","DuLala","female","1992-02-03 00:00:00","18209179577","9",null,null,null},
                {"6","zhiRuo","female","1988-02-03 00:00:00","18209876577","1","1","Zhang Wuji","100"},
                {"7","LingShan","female","1987-12-30 00:00:00","18219876577","9",null,null,null},
                {"8","Xiao Zhao","female","1989-02-03 00:00:00","18209876567","1","1","Zhang Wuji","100"},
                {"9","Shuange","female","1993-02-03 00:00:00","18209876579","9",null,null,null},
                {"10","Wang Yuyan","female","1992-02-03 00:00:00","18209179577","4","4","DuanYU","300"},
                {"11","Xia Xue","female","1993-02-03 00:00:00","18209876579","9",null,null,null},
                {"12","Zhao Min","female","1992-02-03 00:00:00","18209179577","1","1","Zhang Wuji","100"}};
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

    public List<List> expectedLeftList3() {
        String[][] dataArray = {{"120","Tre","200","1700",null},{"130","Cor","205","1700",null},
                {"140","Con","121","1700",null},{"150","Sha","145","1700",null},
                {"160","Ben","100","1700",null},{"170","Man","200","1700",null},
                {"180","Con","108","1700",null},{"190","Con","200","1700",null},
                {"200","Ope","200","1700",null},{"210","IT ","114","1700",null},
                {"220","NOC","103","1700",null},{"230","IT ","145","1700",null},
                {"240","Gov","201","1700",null},{"250","Ret","145","1700",null},
                {"260","Rec","204","1700",null},{"270","Pay","100","1700",null}};
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

    public List<List> expectedLeftList4() {
        String[][] dataArray = {{"1","2","3",null,null}, {"4","5","6",null,null},{"7","8","9",null,null}};
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

    public List<List> expectedLeftList5() {
        String[][] dataArray = {{"1","2","3"}, {"4","5","6"},{"7","8","9"}};
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

    public List<List> expectedLeftList6() {
        String[][] dataArray = {{"1","LiuYan"}, {"2","TeacherLi"},{"5","DuLala"}};
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

    public List<List> expectedLeftList7() {
        String[][] dataArray = {{"2","Lisi",null,null}, {"1","Zhangsan","1","Zhangsan"},
                {"4","Tita",null,null},{"3","Wang Wu","3","Wang Wu"}};
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

    public List<List> expectedRightList1() {
        String[][] dataArray = {{null,"5","Zhang Fei"}, {null,"6","Panan"}};
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

    public List<List> expectedRightList2() {
        String[][] dataArray = {{null,null,null,null,"mahran","20"}, {null,null,null,null,"Jen",null},
                {null,null,null,null,"Gill","20"},{"1","Learn PHP","John Poul","2007-05-24","John Poul","1"},
                {"3","JAVA Tutorial","Sanjay","2007-05-06","Sanjay","1"}};
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

    public List<List> expectedRightList3() {
        String[][] dataArray = {{null,null,null,"3","1"}, {null,null,null,"6","2"}};
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

    public List<List> expectedRightList4() {
        String[][] dataArray = {{null,null,"5","Hello"}, {"1","Zhangsan","1","Zhangsan"},
                {"3","Wang Wu","3","Wang Wu"},{null,null,"6","NiNi"}};
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

    public List<List> expectedRightList5() {
        String[][] dataArray = {{"1","zhangsan","100","100","class-1"}};
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


    @BeforeClass(alwaysRun = true, description = "测试前连接数据库")
    public static void setUpAll() throws SQLException {
        Assert.assertNotNull(TableOuterJoin.connection);
    }

    @Test(priority = 0, enabled = true, description = "验证全外连接查询全部数据")
    public void test01FullOuterJoinAllData() throws SQLException {
        initStudentAndClassTB();
        List<List> expectedList = expectedFullList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListWithOuter = outerJoinObj.fullOuterJoinAll();
        System.out.println("Actual: " + actualListWithOuter);

        Assert.assertTrue(actualListWithOuter.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListWithOuter));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证省略outer")
    public void test02FullOuterJoinOmitOuter() throws SQLException {
        List<List> expectedList = expectedFullList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListOmitOuter = outerJoinObj.fullOuterJoinOmitOuter();
        System.out.println("Actual: " + actualListOmitOuter);

        Assert.assertTrue(actualListOmitOuter.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListOmitOuter));
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证缺少连接条件，执行预期失败")
    public void test03FullOuterJoinWithoutCondition() throws SQLException {
        outerJoinObj.fullOuterJoinWithoutCondition();
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证添加where条件")
    public void test04FullOuterJoinWhereState1() throws SQLException {
        List<List> expectedList = expectedRightList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.fullOuterJoinWhereState();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 4, enabled = true, description = "验证两表没有相同数据时查询全连接数据")
    public void test05FullOuterJoinNoSameData() throws SQLException {
        initProductTB();
        List<List> expectedList = expectedFullList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListNoSameData = outerJoinObj.fullOuterJoinNoSameData();
        System.out.println("Actual: " + actualListNoSameData);

        Assert.assertTrue(actualListNoSameData.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListNoSameData));
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证当一个表为空时，全连接查询")
    public void test06FullOuterJoinOneEmpty() throws SQLException {
        List<List> expectedList = expectedFullList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualListOneEmpty = outerJoinObj.fullOuterJoinOneEmpty();
        System.out.println("Actual: " + actualListOneEmpty);

        Assert.assertTrue(actualListOneEmpty.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListOneEmpty));
    }

    @Test(priority = 6, enabled = true, description = "验证使用using(key)作为条件")
    public void test07FullOuterJoinUsingKeyState() throws SQLException {
        initTestTB();
        List<List> expectedList = expectedFullList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUsingKeyState = outerJoinObj.fullOuterJoinUsingKeyState();
        System.out.println("Actual: " + actualListUsingKeyState);

        Assert.assertTrue(actualListUsingKeyState.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUsingKeyState));
    }

    @Test(priority = 7, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            expectedExceptions = SQLException.class, description = "验证条件字段不存在，执行预期失败")
    public void test08FullOuterJoinWrongKey() throws SQLException {
        outerJoinObj.fullOuterJoinWrongKey();
    }

    @Test(priority = 8, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            description = "验证使用*查询所有")
    public void test09FullOuterJoinStarQueryAll() throws SQLException {
        List<List> expectedList = expectedFullList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListStarQueryAll = outerJoinObj.fullOuterJoinStarQueryAll();
        System.out.println("Actual: " + actualListStarQueryAll);

        Assert.assertTrue(actualListStarQueryAll.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll));
    }

    @Test(priority = 9, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            expectedExceptions = SQLException.class, description = "验证查询相同字段，不使用表名修饰预期失败")
    public void test10FullOuterJoinWrongKey() throws SQLException {
        outerJoinObj.fullOuterJoinSameIDWithoutTablePrefix();
    }

    @Test(priority = 10, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询独有字段可不使用表名修饰")
    public void test11FullOuterJoinUniqueFieldQuery() throws SQLException {
        List<List> expectedList = expectedFullList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUniqueFieldQuery = outerJoinObj.fullOuterJoinUniqueFieldQuery();
        System.out.println("Actual: " + actualListUniqueFieldQuery);

        Assert.assertTrue(actualListUniqueFieldQuery.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUniqueFieldQuery));
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证表名位置互换查询结果相同")
    public void test12FullOuterJoinExchangeTable() throws SQLException {
        List<List> expectedList = expectedFullList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListExchangeTable = outerJoinObj.fullOuterJoinExchangeTable();
        System.out.println("Actual: " + actualListExchangeTable);

        Assert.assertTrue(actualListExchangeTable.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListExchangeTable));
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表匹配到的数据")
    public void test13FullOuterJoinMatchRows() throws SQLException {
        List<List> expectedList = expectedFullList6();
        System.out.println("Expected: " + expectedList);
        List<List> actualListMatchRow = outerJoinObj.fullOuterJoinMatchRow();
        System.out.println("Actual: " + actualListMatchRow);

        Assert.assertTrue(actualListMatchRow.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListMatchRow));
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表未匹配到的数据")
    public void test14FullOuterJoinNotMatchRows() throws SQLException {
        List<List> expectedList = expectedFullList7();
        System.out.println("Expected: " + expectedList);
        List<List> actualListNotMatchRow = outerJoinObj.fullOuterJoinNotMatchRow();
        System.out.println("Actual: " + actualListNotMatchRow);

        Assert.assertTrue(actualListNotMatchRow.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListNotMatchRow));
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证没有相同字段，使用using(key)条件预期失败")
    public void test15FullOuterJoinUsingKeyNoSame() throws SQLException {
        outerJoinObj.fullOuterJoinUsingKeyNoSameField();
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证两表均为空，全连接返回空，无异常")
    public void test16FullOuterJoinBothEmpty() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.fullOuterJoinBothEmpty();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表交叉连接的全部数据")
    public void test17CrossJoinAllData() throws SQLException {
        List<List> expectedList = expectedCrossList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCrossAll = outerJoinObj.crossJoinAll();
        System.out.println("Actual: " + actualListCrossAll);

        Assert.assertTrue(actualListCrossAll.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCrossAll));
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证使用逗号分隔做交叉连接")
    public void test18CrossJoinCommaSeparate() throws SQLException {
        List<List> expectedList = expectedCrossList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCrossComma = outerJoinObj.crossJoinAllSeprateComma();
        System.out.println("Actual: " + actualListCrossComma);

        Assert.assertTrue(actualListCrossComma.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCrossComma));
    }

    @Test(priority = 18, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            expectedExceptions = SQLException.class, description = "验证交叉连接不允许使用连接条件")
    public void test19CrossJoinExtraCondition() throws SQLException {
        outerJoinObj.crossJoinExtraCondition();
    }

    @Test(priority = 19, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证连接后使用where条件过滤")
    public void test20CrossJoinWhereCondition() throws SQLException {
        List<List> expectedList = expectedCrossList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListWhereCondition = outerJoinObj.crossJoinWhereCondition();
        System.out.println("Actual: " + actualListWhereCondition);

        Assert.assertTrue(actualListWhereCondition.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListWhereCondition));
    }

    @Test(priority = 20, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证使用逗号分隔交叉连接后使用where条件过滤")
    public void test21CrossJoinCommaWhereCondition() throws SQLException {
        List<List> expectedList = expectedCrossList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualListCommaWhereCondition = outerJoinObj.crossJoinCommaWhereCondition();
        System.out.println("Actual: " + actualListCommaWhereCondition);

        Assert.assertTrue(actualListCommaWhereCondition.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListCommaWhereCondition));
    }

    @Test(priority = 21, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证一表为空，交叉连接返回空，无异常")
    public void test22CrossJoinOneEmpty() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.crossJoinOneEmpty();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 22, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            description = "验证使用*查询交叉连接全部数据")
    public void test23CrossJoinStarQueryAll() throws SQLException {
        List<List> expectedList = expectedCrossList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualListStarQueryAll1 = outerJoinObj.crossJoinStarQueryAll1();
        System.out.println("Actual: " + actualListStarQueryAll1);
        Assert.assertTrue(actualListStarQueryAll1.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll1));

        List<List> actualListStarQueryAll2 = outerJoinObj.crossJoinStarQueryAll2();
        System.out.println("Actual: " + actualListStarQueryAll2);
        Assert.assertTrue(actualListStarQueryAll2.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListStarQueryAll2));
    }

    @Test(priority = 23, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            expectedExceptions = SQLException.class, description = "验证交叉连查询相同id不使用表名修饰，预期失败")
    public void test24CrossJoinSameFieldNoTablePrefix() throws SQLException {
        outerJoinObj.crossJoinSameFieldNoTablePrefix();
    }

    @Test(priority = 24, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证查询两表独有字段不需使用表名修饰")
    public void test25CrossJoinUniqueFieldNoTablePrefix() throws SQLException {
        List<List> expectedList = expectedCrossList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualListUniqueField = outerJoinObj.crossJoinUniqueFieldNoTablePrefix();
        System.out.println("Actual: " + actualListUniqueField);

        Assert.assertTrue(actualListUniqueField.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListUniqueField));
    }

    @Test(priority = 25, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证调换位置做交叉连接，笛卡尔积结果不同")
    public void test26CrossJoinExchangeTable() throws SQLException {
        List<List> expectedList = expectedCrossList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualListExchangeTable = outerJoinObj.crossJoinExchangeTable();
        System.out.println("Actual: " + actualListExchangeTable);

        Assert.assertTrue(actualListExchangeTable.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualListExchangeTable));
    }


    @Test(priority = 26, enabled = true, description = "验证左连接仅查询在左表的数据")
    public void test27LeftJoinOnlyInLeftTable() throws SQLException {
        initGirlsTB();
        List<List> expectedList = expectedLeftList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinOnlyInLeft();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 27, enabled = true, dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            description = "验证左连接全部数据")
    public void test28LeftJoinAllData() throws SQLException {
        List<List> expectedList = expectedLeftList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinAllData();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 28, enabled = true, description = "验证左连接省略outer")
    public void test29LeftJoinOmitOuter() throws SQLException {
        initEmployeesTB();
        List<List> expectedList = expectedLeftList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftOmiterOuterList = outerJoinObj.leftOuterJoinOmitOuter();
        System.out.println("Actual: " + actualLeftOmiterOuterList);

        Assert.assertTrue(actualLeftOmiterOuterList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftOmiterOuterList));
    }

    @Test(priority = 29, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            description = "验证左连接两表无交集")
    public void test30LeftJoinNoSameData() throws SQLException {
        List<List> expectedList = expectedLeftList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinNoSameData();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 30, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            expectedExceptions = SQLException.class, description = "验证连接字段在一个表中不存在")
    public void test31LeftJoinKeyNotExist() throws SQLException {
        outerJoinObj.leftOuterJoinWrongKey();
    }

    @Test(priority = 31, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            description = "验证左表为空，返回空，无异常")
    public void test32LeftJoinNoDataLeft() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.leftOuterJoinNoDataLeft();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 32, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            description = "右表无数据，左表可返回全部")
    public void test33LeftJoinNoDataRight() throws SQLException {
        List<List> expectedList = expectedLeftList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinNoDataRight();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 33, enabled = true, expectedExceptions = SQLException.class,
            dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            description = "缺少连接条件，预期异常")
    public void test34LeftJoinMissingCondition() throws SQLException {
        outerJoinObj.leftOuterJoinMissingCondition();
    }

    @Test(priority = 34, enabled = true, dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            description = "验证添加where条件")
    public void test35LeftJoinWhereState1() throws SQLException {
        List<List> expectedList = expectedLeftList6();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinWhereState1();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 35, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证添加where条件")
    public void test36LeftJoinWhereState2() throws SQLException {
        List<List> expectedList = expectedRightList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinWhereState2();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }

    @Test(priority = 36, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            description = "验证连接使用using(key)")
    public void test37LeftJoinUsingKey() throws SQLException {
        List<List> expectedList = expectedLeftList7();
        System.out.println("Expected: " + expectedList);
        List<List> actualLeftList = outerJoinObj.leftOuterJoinUsingKey();
        System.out.println("Actual: " + actualLeftList);

        Assert.assertTrue(actualLeftList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualLeftList));
    }


    @Test(priority = 37, enabled = true, dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            description = "验证右连接仅查询在右表的数据")
    public void test38RightJoinOnlyInRightTable() throws SQLException {
        List<List> expectedList = expectedRightList1();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinOnlyInRight();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 38, enabled = true, description = "验证右连接全部表数据")
    public void test39RightJoinAllData() throws SQLException {
        initW3cTB();
        List<List> expectedList = expectedRightList2();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinAllData();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 39, enabled = true, dependsOnMethods = {"test29LeftJoinOmitOuter"}, description = "验证右连接省略outer")
    public void test40RightJoinOmitOuter() throws SQLException {
        List<List> expectedList = expectedLeftList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinOmitOuter();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 40, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"}, description = "验证右连接两表无交集")
    public void test41RightJoinNoSameData() throws SQLException {
        List<List> expectedList = expectedRightList3();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinNoSameData();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 41, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            expectedExceptions = SQLException.class, description = "验证连接字段在一个表中不存在")
    public void test42RightJoinKeyNotExist() throws SQLException {
        outerJoinObj.rightOuterJoinWrongKey();
    }

    @Test(priority = 42, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            description = "左表无数据，右表可返回全部")
    public void test43RightJoinNoDataLeft() throws SQLException {
        List<List> expectedList = expectedLeftList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinNoDataLeft();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 43, enabled = true, dependsOnMethods = {"test05FullOuterJoinNoSameData"},
            description = "验证右表为空，返回空，无异常")
    public void test44RightJoinNoDataRight() throws SQLException {
        Boolean actualRowReturn = outerJoinObj.rightOuterJoinNoDataRight();
        System.out.println(actualRowReturn);
        Assert.assertFalse(actualRowReturn);
    }

    @Test(priority = 44, enabled = true, dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            description = "验证添加where条件")
    public void test45RightJoinWhereState1() throws SQLException {
        String expectedStr = "Panan";
        System.out.println("Expected: " + expectedStr);
        String actualRightStr = outerJoinObj.rightOuterJoinWhereState1();
        System.out.println("Actual: " + actualRightStr);

        Assert.assertEquals(actualRightStr, expectedStr);
    }

    @Test(priority = 45, enabled = true, dependsOnMethods = {"test01FullOuterJoinAllData"},
            description = "验证添加where条件")
    public void test46RightJoinWhereState2() throws SQLException {
        List<List> expectedList = expectedRightList5();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinWhereState2();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }

    @Test(priority = 46, enabled = true, dependsOnMethods = {"test27LeftJoinOnlyInLeftTable"},
            expectedExceptions = SQLException.class, description = "缺少连接条件，预期异常")
    public void test47RightJoinMissingCondition() throws SQLException {
        outerJoinObj.rightOuterJoinMissingCondition();
    }

    @Test(priority = 47, enabled = true, dependsOnMethods = {"test07FullOuterJoinUsingKeyState"},
            description = "验证连接使用using(key)")
    public void test48RightJoinUsingKey() throws SQLException {
        List<List> expectedList = expectedRightList4();
        System.out.println("Expected: " + expectedList);
        List<List> actualRightList = outerJoinObj.rightOuterJoinUsingKey();
        System.out.println("Actual: " + actualRightList);

        Assert.assertTrue(actualRightList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualRightList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = TableOuterJoin.connection.createStatement();
        tearDownStatement.execute("delete from student_tbl");
        tearDownStatement.execute("drop table student_tbl");
        tearDownStatement.execute("delete from class_tbl");
        tearDownStatement.execute("drop table class_tbl");
        tearDownStatement.execute("delete from student_tbl1");
        tearDownStatement.execute("drop table student_tbl1");
        tearDownStatement.execute("delete from class_tbl1");
        tearDownStatement.execute("drop table class_tbl1");
        tearDownStatement.execute("delete from product1");
        tearDownStatement.execute("drop table product1");
        tearDownStatement.execute("delete from product2");
        tearDownStatement.execute("drop table product2");
        tearDownStatement.execute("delete from test1");
        tearDownStatement.execute("drop table test1");
        tearDownStatement.execute("delete from test2");
        tearDownStatement.execute("drop table test2");
        tearDownStatement.execute("delete from beauty_tbl");
        tearDownStatement.execute("drop table beauty_tbl");
        tearDownStatement.execute("delete from boys_tbl");
        tearDownStatement.execute("drop table boys_tbl");
        tearDownStatement.execute("delete from boys_right");
        tearDownStatement.execute("drop table boys_right");
        tearDownStatement.execute("delete from product3");
        tearDownStatement.execute("drop table product3");
        tearDownStatement.execute("delete from w3cschool_tbl");
        tearDownStatement.execute("drop table w3cschool_tbl");
        tearDownStatement.execute("delete from tcount_tbl");
        tearDownStatement.execute("drop table tcount_tbl");
        tearDownStatement.execute("delete from departments_tbl");
        tearDownStatement.execute("drop table departments_tbl");
        tearDownStatement.execute("delete from employees_tbl");
        tearDownStatement.execute("drop table employees_tbl");

        tearDownStatement.close();
        TableOuterJoin.connection.close();
    }
}
