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

import io.dingodb.dailytest.BetweenState;
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

public class TestBetweenAndState extends YamlDataHelper {
    public static BetweenState betweenObj = new BetweenState();

    public void initBetweenTable1() throws SQLException {
        String between_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String between_value1_path = "src/test/resources/testdata/tableInsertValues/between_and_value1.txt";
        String between_meta1 = FileReaderUtil.readFile(between_meta1_path);
        String between_value1 = FileReaderUtil.readFile(between_value1_path);
        betweenObj.betweenTable1Create(between_meta1);
        betweenObj.insertTable1Values(between_value1);
    }

    public void initBetweenTable2() throws SQLException {
        String between_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String between_value2_path = "src/test/resources/testdata/tableInsertValues/between_and_value2.txt";
        String between_meta1 = FileReaderUtil.readFile(between_meta1_path);
        String between_value2 = FileReaderUtil.readFile(between_value2_path);
        betweenObj.betweenTable2Create(between_meta1);
        betweenObj.insertTable2Values(between_value2);
    }

    public void initBetweenTable3() throws SQLException {
        String between_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String between_value3_path = "src/test/resources/testdata/tableInsertValues/between_and_value3.txt";
        String between_meta1 = FileReaderUtil.readFile(between_meta1_path);
        String between_value3 = FileReaderUtil.readFile(between_value3_path);
        betweenObj.betweenTable3Create(between_meta1);
        betweenObj.insertTable3Values(between_value3);
    }

    public void initBetweenTable4() throws SQLException {
        String between_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String between_value4_path = "src/test/resources/testdata/tableInsertValues/between_and_value4.txt";
        String between_meta1 = FileReaderUtil.readFile(between_meta1_path);
        String between_value4 = FileReaderUtil.readFile(between_value4_path);
        betweenObj.betweenTable4Create(between_meta1);
        betweenObj.insertTable4Values(between_value4);
    }

    public void initBetweenTable5() throws SQLException {
        String between_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String between_value1_path = "src/test/resources/testdata/tableInsertValues/between_and_value1.txt";
        String between_meta1 = FileReaderUtil.readFile(between_meta1_path);
        String between_value1 = FileReaderUtil.readFile(between_value1_path);
        betweenObj.betweenTable5Create(between_meta1);
        betweenObj.insertTable5Values(between_value1);
    }

    public void initBetweenTable6And7() throws SQLException {
        String between_meta2_path = "src/test/resources/testdata/tablemeta/between_and_meta2.txt";
        String between_value6_path = "src/test/resources/testdata/tableInsertValues/between_and_value5.txt";
        String between_meta2 = FileReaderUtil.readFile(between_meta2_path);
        String between_value6 = FileReaderUtil.readFile(between_value6_path);
        betweenObj.betweenTable6Create(between_meta2);
        betweenObj.insertTable6Values(between_value6);

        String between_meta3_path = "src/test/resources/testdata/tablemeta/job_grades_meta.txt";
        String between_value7_path = "src/test/resources/testdata/tableInsertValues/between_and_value6.txt";
        String between_meta3 = FileReaderUtil.readFile(between_meta3_path);
        String between_value7 = FileReaderUtil.readFile(between_value7_path);
        betweenObj.betweenTable7Create(between_meta3);
        betweenObj.insertTable7Values(between_value7);

    }

    public static List<List> expectedTest01List() {
        String[][] dataArray = {{"3", "li3"},{"4", "HAHA"}, {"5", "awJDs"}, {"6", "123"}, {"7", "yamaha"}};
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest02List() {
        String[][] dataArray = {
                {"2", "lisi", "25"},{"3", "li3", "55"}, {"4", "HAHA", "57"}, {"6", "123", "60"},
                {"8", "wangwu", "44"},{"9", "Steven", "20"},{"10", "3M", "31"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest03List() {
        String[][] dataArray = {
                {"2", "lisi", "895.0"},{"3", "li3", "123.123"}, {"5", "awJDs", "1453.9999"},
                {"8", "wangwu", "1000.0"},{"9", "Steven", "2000.0"},{"14", "Sity", "2000.0"},
                {"15", "Public", "100.0"},{"16", "Juliya", "1999.99"},{"17", "1.5", "500.0"},
                {"19", "Adidas", "1453.9999"},{"20", "Bilibili", "100.0"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest04List1() {
        String[][] dataArray = {
                {"2", "lisi"},{"3", "li3"}, {"4", "HAHA"},{"11", "GiGi"},
                {"12", "Kelay"},{"16", "Juliya"},{"20", "Bilibili"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest04List2() {
        String[][] dataArray = {{"10", "3M"},{"18", "777"}};
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest04List3() {
        String[][] dataArray = {
                {"2", "lisi", "haidian"},{"4", "HAHA", "CHANGping"}, {"7", "yamaha", "beijing changyang"},
                {"11", "GiGi", "Huhe"},{"14", "Sity", "beijing changyang"}, {"16", "Juliya", "Huluodao"},
                {"17", "1.5", "JinMen"},{"21", "Zala", "JiZhou"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest05List() {
        String[][] dataArray = {
                {"4", "HAHA", "2020-11-11"}, {"5", "awJDs", "2010-10-01"}, {"8", "wangwu", "2015-09-10"},
                {"12", "Kelay", "2018-05-31"}, {"13", " Nigula", "2014-10-13"},{"18", "777", "2020-11-11"},
                {"19", "Adidas", "2010-10-01"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest06List() {
        String[][] dataArray = {
                {"1", "zhangsan", "08:10:10"}, {"2", "lisi", "06:15:08"}, {"3", "li3", "07:03:15"},
                {"5", "awJDs", "19:00:00"}, {"9", "Steven", "16:35:38"}, {"10", "3M", "17:30:15"},
                {"11", "GiGi", "06:00:00"},{"14", "Sity", "12:30:00"}, {"16", "Juliya", "14:09:49"},
                {"17", "1.5", "15:20:20"},{"19", "Adidas", "19:00:00"}, {"20", "Bilibili", "11:11:00"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest07List() {
        String[][] dataArray = {
                {"1", "zhangsan", "2022-04-08 18:05:07"}, {"2", "lisi", "2000-02-29 00:00:00"},
                {"4", "HAHA", "2021-05-04 12:00:00"},{"5", "awJDs", "2010-10-01 02:02:02"},
                {"8", "wangwu", "2001-11-11 18:05:07"}, {"9", "Steven", "2008-08-08 08:00:00"},
                {"12", "Kelay", "2000-01-01 00:00:00"},{"15", "Public", "2020-02-29 05:53:44"},
                {"16", "Juliya", "2000-02-29 00:00:00"}, {"18", "777", "2021-05-04 12:00:00"},
                {"19", "Adidas", "2010-10-01 02:02:02"},{"21", "Zala", "2022-07-07 13:30:03"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest12List() {
        String[][] dataArray = {
                {"1", "zhangsan"},{"2", "lisi"}, {"18", "777"},
                {"19", "Adidas"}, {"20", "Bilibili"},{"21", "Zala"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest13List() {
        String[][] dataArray = {
                {"1", "zhangsan","18"},{"5", "awJDs", "1"},{"7", "yamaha", "76"},{"11", "GiGi", "98"},
                {"12", "Kelay", "10"},{"13", " Nigula", "100"},{"14", "Sity", "15"},{"15", "Public", "18"},
                {"16", "Juliya", "82"},{"17", "1.5", "120"},{"18", "777", "77"},{"19", "Adidas", "1"},
                {"20", "Bilibili", "200"},{"21", "Zala", "76"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest14List() {
        String[][] dataArray = {
                {"1", "zhangsan","23.5"},{"4", "HAHA", "9.0762556"},{"6", "123", "0.0"},{"7", "yamaha", "2.3"},
                {"10", "3M", "20010.0"},{"11", "GiGi", "4201.98"},{"12", "Kelay", "87231.0"},{"13", " Nigula", "98.99"},
                {"18", "77", "77.77"},{"21", "Zala", "2000.01"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest15List1() {
        String[][] dataArray = {
                {"1", "zhangsan"},{"5", "awJDs"},{"6", "123"},{"7", "yamaha"},{"8", "wangwu"},
                {"9", "Steven"},{"10", "3M"},{"13", " Nigula"},{"14", "Sity"},{"15", "Public"},{"17", "1.5"},
                {"18", "77"},{"19", "Adidas"},{"21", "Zala"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest15List2() {
        String[][] dataArray = {
                {"1", "zhangsan"},{"2", "lisi"},{"3", "li3"},{"4", "HAHA"}, {"5", "awJDs"},
                {"6", "123"},{"7", "yamaha"},{"8", "wangwu"}, {"9", "Steven"},{"11", "GiGi"},{"12", "Kelay"},
                {"13", " Nigula"},{"14", "Sity"},{"15", "Public"},{"16", "Juliya"},{"17", "1.5"},
                {"19", "Adidas"},{"20", "Bilibili"},{"21", "Zala"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest15List3() {
        String[][] dataArray = {
                {"1", "zhangsan", "beijing"},{"3", "li3", "wuhan NO.1 Street"},{"5", "awJDs", "pingYang1"},
                {"6", "123", "543"},{"8", "wangwu", "beijing"}, {"9", "Steven", " beijing haidian "},{"10", "3M", "Lasa"},
                {"12", "Kelay", "Yang GU"}, {"13", " Nigula", "Alaska"},{"15", "Public", "beijing"},{"18", "777", "7788"},
                {"19", "Adidas", "pingYang1"},{"20", "Bilibili", "Xuchang"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest16List() {
        String[][] dataArray = {
                {"1", "zhangsan", "1998-04-06"},{"2", "lisi", "1988-02-05"},
                {"3", "li3", "2022-03-04"}, {"6", "123", "1987-07-16"},{"7", "yamaha", "1949-01-01"},
                {"9", "Steven", "1995-12-15"},{"10", "3M", "2021-03-04"}, {"11", "GiGi", "1976-07-07"},
                {"14", "Sity", "1949-10-01"},{"15", "Public", "2007-08-15"},{"16", "Juliya", "1960-11-11"},
                {"17", "1.5", "2022-03-01"},{"20", "Bilibili", "1987-12-11"},{"21", "Zala", "2022-07-07"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest17List() {
        String[][] dataArray = {
                {"4", "HAHA", "05:59:59"},{"6", "123", "01:02:03"},{"7", "yamaha", "00:30:08"},
                {"8", "wangwu", "03:45:10"},{"12", "Kelay", "21:00:00"}, {"13", " Nigula", "01:00:00"},
                {"15", "Public", "22:10:10"},{"18", "777", "05:59:59"}, {"21", "Zala", "00:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest18List() {
        String[][] dataArray = {
                {"3", "li3", "1999-02-28 23:59:59"},{"6", "123", "1952-12-31 12:12:12"},
                {"7", "yamaha", "2022-12-01 01:02:03"}, {"10", "3M", "1999-02-28 00:59:59"},
                {"11", "GiGi", "2024-05-04 12:00:00"}, {"13", " Nigula", "1999-12-31 23:59:59"},
                {"14", "Sity", "2022-12-31 23:59:59"},{"17", "1.5", "1953-10-21 16:10:28"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest22List() {
        String[][] dataArray = {
                {"3", "li3", "2022-03-04"}, {"4", "HAHA", "2020-11-11"},{"8", "wangwu", "2015-09-10"},
                {"10", "3M", "2021-03-04"}, {"12", "Kelay", "2018-05-31"}, {"13", " Nigula", "2014-10-13"},
                {"17", "1.5", "2022-03-01"},{"18", "777", "2020-11-11"},{"21", "Zala", "2022-07-07"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest23List() {
        String[][] dataArray = {
                {"1", "zhangsan", "1998-04-06"}, {"2", "lisi", "1988-02-05"},{"6", "123", "1987-07-16"},
                {"7", "yamaha", "1949-01-01"}, {"9", "Steven", "1995-12-15"}, {"11", "GiGi", "1976-07-07"},
                {"14", "Sity", "1949-10-01"},{"16", "Juliya", "1960-11-11"},{"20", "Bilibili", "1987-12-11"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest24List() {
        String[][] dataArray = {
                {"5", "awJDs", "19:00:00"},{"9", "Steven", "16:35:38"},{"10", "3M", "17:30:15"},
                {"12", "Kelay", "21:00:00"},{"15", "Public", "22:10:10"},{"16", "Juliya", "14:09:49"},
                {"17", "1.5", "15:20:20"},{"19", "Adidas", "19:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest25List() {
        String[][] dataArray = {
                {"1", "zhangsan", "08:10:10"},{"2", "lisi", "06:15:08"},{"3", "li3", "07:03:15"},
                {"4", "HAHA", "05:59:59"},{"6", "123", "01:02:03"},{"7", "yamaha", "00:30:08"},
                {"8", "wangwu", "03:45:10"},{"11", "GiGi", "06:00:00"}, {"13", " Nigula", "01:00:00"},
                {"18", "777", "05:59:59"},{"20", "Bilibili", "11:11:00"}, {"21", "Zala", "00:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest26List() {
        String[][] dataArray = {
                {"1", "zhangsan", "2022-04-08 18:05:07"},{"4", "HAHA", "2021-05-04 12:00:00"},
                {"7", "yamaha", "2022-12-01 01:02:03"}, {"11", "GiGi", "2024-05-04 12:00:00"},
                {"14", "Sity", "2022-12-31 23:59:59"},{"15", "Public", "2020-02-29 05:53:44"},
                {"18", "777", "2021-05-04 12:00:00"},{"21", "Zala", "2022-07-07 13:30:03"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest27List() {
        String[][] dataArray = {{"11", "GiGi", "2024-05-04 12:00:00"}, {"14", "Sity", "2022-12-31 23:59:59"}};
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest28List() {
        String[][] dataArray = {
                {"3", "li3", "1999-02-28 23:59:59"},{"6", "123", "1952-12-31 12:12:12"},
                {"10", "3M", "1999-02-28 00:59:59"},{"12", "Kelay", "2000-01-01 00:00:00"},
                {"13", " Nigula", "1999-12-31 23:59:59"}, {"17", "1.5", "1953-10-21 16:10:28"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest29List() {
        String[][] dataArray = {
                {"2", "lisi", "2000-02-29 00:00:00"},{"3", "li3", "1999-02-28 23:59:59"},
                {"5", "awJDs", "2010-10-01 02:02:02"},{"6", "123", "1952-12-31 12:12:12"},
                {"8", "wangwu", "2001-11-11 18:05:07"}, {"9", "Steven", "2008-08-08 08:00:00"},
                {"10", "3M", "1999-02-28 00:59:59"},{"12", "Kelay", "2000-01-01 00:00:00"},
                {"13", " Nigula", "1999-12-31 23:59:59"},{"16", "Juliya", "2000-02-29 00:00:00"},
                {"17", "1.5", "1953-10-21 16:10:28"}, {"19", "Adidas", "2010-10-01 02:02:02"},
                {"20", "Bilibili", "1997-07-01 00:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest35List() {
        String[][] dataArray = {
                {"2", "lisi", "25"},{"4", null, "57"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest36List() {
        String[][] dataArray = {{"1", "zhangsan"}};
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest37List() {
        String[][] dataArray = {{"1", "zhangsan", "18"},{"2", "lisi", "25"}};
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest40List() {
        String[][] dataArray = {{"1", "zhangsan", "18"},{"4", "HAHA", "25"}, {"6", "123", "20"}};
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest41List() {
        String[][] dataArray = {{"3", "li3", "33"},{"5", "awJDs", "44"}};
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest42List() {
        String[][] dataArray = {
                {"8", "wangwu", "44", "1000.0", "beijing", "2015-09-10", "03:45:10", "1999-02-28 23:59:59", "true"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest43List() {
        String[][] dataArray = {
                {"16", "Juliya", "82", "1999.99", "Huluodao", "1960-11-11", "14:09:49", "2000-02-29 00:00:00", "false"},
                {"19", "Adidas", "1", "1453.9999", "pingYang1", "2010-10-01", "19:00:00", "2010-10-01 02:02:02", "false"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest44List() {
        String[][] dataArray = {
                {"6", "123", "60", "0.0", "543", "1987-07-16", "01:02:03", "1952-12-31 12:12:12", "true"},
                {"10", "3M", "31", "20010.0", "Lasa", "2021-03-04", "17:30:15", "1999-02-28 00:59:59", "false"},
                {"3", "li3", "55", "123.123", "wuhan NO.1 Street", "2022-03-04", "07:03:15", "1999-02-28 23:59:59", "false"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest45List() {
        String[][] dataArray = {
                {"8", "wangwu", "44", "1000.0", "beijing", "2015-09-10", "03:45:10", "1999-02-28 23:59:59", "true"},
                {"14", "Sity", "15", "2000.0", "beijing changyang", "1949-10-01", "12:30:00", "2022-12-31 23:59:59", "true"},
                {"15", "Public", "18", "100.0", "beijing", "2007-08-15", "22:10:10", "2020-02-29 05:53:44", "true"},
                {"7", "yamaha", "76", "2.3", "beijing changyang", "1949-01-01", "00:30:08", "2022-12-01 01:02:03", "false"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest46List() {
        List betweenList = new ArrayList();
        betweenList.add("68");
        betweenList.add("117773.7299");
        betweenList.add("1949-10-01");

        return betweenList;
    }

    public static List<List> expectedTest47List() {
        List notBetweenList = new ArrayList();
        notBetweenList.add("2201.22");
        notBetweenList.add("33018.24");
        notBetweenList.add("17:30:15");
        notBetweenList.add("1952-12-31 12:12:12");

        return notBetweenList;
    }

    public static List<List> expectedTest48List() {
        String[][] dataArray = {
                {"beijing", "1123.5"},{"CHANGping", "9.0762556"},
                {"beijing changyang", "2002.3"},{"Alaska", "98.99"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest49List() {
        String[][] dataArray = {
                {"pingYang1", "1"},{"beijing", "31"},
                {"wuhan NO.1 Street", "55"}, {"beijing changyang", "76"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest50List() {
        String[][] dataArray = {
                {"1", "Java", "18", "BJ"},{"2", "Java", "25", "BJ"},
                {"3", "Java", "55", "BJ"},{"4", "Java", "57", "BJ"},
                {"5", "Java", "1", "BJ"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest51List() {
        String[][] dataArray = {
                {"6", "123", "35", "1234.5678"},{"7", "yamaha", "35", "1234.5678"},
                {"8", "wangwu", "35", "1234.5678"},{"9", "Steven", "35", "1234.5678"},
                {"10", "3M", "35", "1234.5678"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest52List() {
        String[][] dataArray = {
                {"11", "GiGi", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"12", "Kelay", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"13", " Nigula", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"14", "Sity", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"},
                {"15", "Public", "2022-07-13", "11:48:06", "2022-08-01 00:00:00"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest53List() {
        String[][] dataArray = {
                {"16", "Juliya", "true"},{"17", "1.5", "true"},{"18", "777", "true"},
                {"19", "Adidas", "true"},{"20", "Bilibili", "true"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest54List() {
        String[][] dataArray = {
                {"6", "C++", "35", "SH"},{"7", "C++", "35", "SH"},
                {"8", "C++", "35", "SH"},{"9", "C++", "35", "SH"},
                {"10", "C++", "35", "SH"},{"11", "C++", "98", "SH"},
                {"12", "C++", "10", "SH"},{"13", "C++", "100", "SH"},
                {"14", "C++", "15", "SH"},{"15", "C++", "18", "SH"},
                {"16", "C++", "82", "SH"},{"17", "C++", "120", "SH"},
                {"18", "C++", "77", "SH"},{"19", "C++", "1", "SH"},
                {"20", "C++", "200", "SH"},{"21", "C++", "76", "SH"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest55List() {
        String[][] dataArray = {
                {"1", "Java", "66", "7890.0123"},{"2", "Java", "66", "7890.0123"},
                {"3", "Java", "66", "7890.0123"},{"4", "Java", "66", "7890.0123"},
                {"5", "Java", "66", "7890.0123"},{"11", "C++", "66", "7890.0123"},
                {"12", "C++", "66", "7890.0123"},{"13", "C++", "66", "7890.0123"},
                {"14", "C++", "66", "7890.0123"},{"15", "C++", "66", "7890.0123"},
                {"16", "C++", "66", "7890.0123"},{"17", "C++", "66", "7890.0123"},
                {"18", "C++", "66", "7890.0123"},{"19", "C++", "66", "7890.0123"},
                {"20", "C++", "66", "7890.0123"},{"21", "C++", "66", "7890.0123"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest56List() {
        String[][] dataArray = {
                {"1", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"2", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"3", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"4", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"5", "Java", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"6", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"7", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"8", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"9", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"10", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"16", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"17", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"18", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"19", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"20", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"},
                {"21", "C++", "1987-06-18", "23:59:59", "2022-07-31 12:00:00"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest57List() {
        String[][] dataArray = {
                {"1", "Java", "false"}, {"2", "Java", "false"}, {"3", "Java", "false"},
                {"4", "Java", "false"}, {"5", "Java", "false"}, {"6", "C++", "false"},
                {"7", "C++", "false"}, {"8", "C++", "false"}, {"9", "C++", "false"},
                {"10", "C++", "false"}, {"11", "C++", "false"}, {"12", "C++", "false"},
                {"13", "C++", "false"}, {"14", "C++", "false"}, {"15", "C++", "false"},
                {"21", "C++", "false"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest60List() {
        String[][] dataArray = {
                {"6", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"7", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"8", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"9", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"10", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"11", "C++", "66", "7980.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"12", "C++", "66", "7980.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"13", "C++", "66", "7980.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"14", "C++", "66", "7980.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"15", "C++", "66", "7980.0123", "SH", "2022-07-13", "11:48:06", "2022-08-01 00:00:00", "false"},
                {"16", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"17", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"18", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"19", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"20", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "true"},
                {"21", "C++", "66", "7980.0123", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest61List() {
        String[][] dataArray = {
                {"6", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"7", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"8", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"9", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"},
                {"10", "C++", "35", "1234.5678", "SH", "1987-06-18", "23:59:59", "2022-07-31 12:00:00", "false"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }

    public static List<List> expectedTest64List() {
        String[][] dataArray = {
                {"100", "10000", "B"},{"101", "7386", "A"}, {"103", "18350", "C"}
        };
        List<List> betweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            betweenList.add(columnList);
        }
        return betweenList;
    }

    public static List<List> expectedTest65List() {
        String[][] dataArray = {
                {"10000", "A"},{"10000", "C"}, {"7386", "B"},{"7386", "C"},
                {"20001", "A"},{"20001", "B"},{"20001", "C"},{"18350", "A"},
                {"18350", "B"},{"4500", "A"},{"4500", "B"},{"4500", "C"}
        };
        List<List> notBetweenList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            notBetweenList.add(columnList);
        }
        return notBetweenList;
    }


    @BeforeClass(alwaysRun = true, description = "????????????????????????")
    public static void setupAll() {
        Assert.assertNotNull(BetweenState.connection);
    }

    @Test(enabled = true, description = "???????????????1")
    public void test00CreateBetweenTable1() throws SQLException {
        initBetweenTable1();
    }

    @Test(enabled = true, description = "???????????????2")
    public void test00CreateBetweenTable2() throws SQLException {
        initBetweenTable2();
    }

    @Test(enabled = true, description = "???????????????3")
    public void test00CreateBetweenTable3() throws SQLException {
        initBetweenTable3();
    }

    @Test(enabled = true, description = "???????????????4")
    public void test00CreateBetweenTable4() throws SQLException {
        initBetweenTable4();
    }

    @Test(enabled = true, description = "???????????????5")
    public void test00CreateBetweenTable5() throws SQLException {
        initBetweenTable5();
    }

    @Test(enabled = true, description = "???????????????6???7")
    public void test00CreateBetweenTable67() throws SQLException {
        initBetweenTable6And7();
    }

    @Test(priority = 0, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????between and???????????????????????????")
    public void test01BetweenQueryByPrimaryKeyRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest01List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByPrimaryKeyRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between????????????????????????????????????")
    public void test02BetweenQueryByIntRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest02List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByIntRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???????????????????????????????????????")
    public void test03BetweenQueryByDoubleRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest03List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByDoubleRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???????????????????????????????????????")
    public void test04BetweenQueryByStrRange1() throws SQLException {
        List<List> expectedBetweenList = expectedTest04List1();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange1();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???????????????????????????????????????")
    public void test04BetweenQueryByStrRange2() throws SQLException {
        List<List> expectedBetweenList = expectedTest04List2();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange2();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???????????????????????????????????????")
    public void test04BetweenQueryByStrRange3() throws SQLException {
        List<List> expectedBetweenList = expectedTest04List3();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByStrRange3();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???Date???????????????????????????")
    public void test05BetweenQueryByDateRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest05List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByDateRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???Time???????????????????????????")
    public void test06BetweenQueryByTimeRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest06List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByTimeRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???TimeStamp???????????????????????????")
    public void test07BetweenQueryByTimeStampRange() throws SQLException {
        List<List> expectedBetweenList = expectedTest07List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryByTimeStampRange();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between????????????????????????????????????????????????")
    public void test08BetweenStartGTEnd(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryStartGTEnd(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 7, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between????????????????????????????????????")
    public void test08BetweenStartEQEnd(Map<String, String> param) throws SQLException {
        StrTo2DList strTo2DList = new StrTo2DList();
        List<List> expectedBetweenList = strTo2DList.construct2DList(param.get("dataStr"));
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryStartEQEnd(param.get("betweenState"), param.get("testField"));
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 8, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???????????????????????????")
    public void test09BetweenQueryFullRange(Map<String, String> param) throws SQLException {
        int actualQueryRows = betweenObj.betweenQueryByFullRange(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryRows);

        Assert.assertEquals(actualQueryRows, 21);
    }

    @Test(priority = 9, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????????????????????????????????????????????????????")
    public void test10BetweenQueryInvalidDateTime(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryInvalidDateTime(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 10, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????????????????????????????????????????")
    public void test11BetweenNoValueMatch(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryNoValueMatched(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 11, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????not between and???????????????????????????")
    public void test12NotBetweenQueryByPrimaryKeyRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest12List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByPrimaryKeyRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????not between and???????????????????????????")
    public void test13NotBetweenQueryByIntRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest13List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByIntRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???????????????????????????????????????")
    public void test14NotBetweenQueryByDoubleRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest14List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByDoubleRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }


    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????NotBetween???????????????????????????????????????")
    public void test15NotBetweenQueryByStrRange1() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest15List1();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????NotBetween???????????????????????????????????????")
    public void test15NotBetweenQueryByStrRange2() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest15List2();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 14, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????NotBetween???????????????????????????????????????")
    public void test15NotBetweenQueryByStrRange3() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest15List3();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByStrRange3();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???Date???????????????????????????")
    public void test16NotBetweenQueryByDateRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest16List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByDateRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 16, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???Time???????????????????????????")
    public void test17NotBetweenQueryByTimeRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest17List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByTimeRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???TimeStamp???????????????????????????")
    public void test18NotBetweenQueryByTimeStampRange() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest18List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryByTimeStampRange();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 18, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????????????????????????????????????????")
    public void test19NotBetweenStartGTEnd(Map<String, String> param) throws SQLException {
        int actualQueryRows = betweenObj.notBetweenQueryStartGTEnd(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryRows);

        Assert.assertEquals(actualQueryRows, 21);
    }

    @Test(priority = 18, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between??????????????????????????????????????????????????????????????????")
    public void test19NotBetweenStartEQEnd(Map<String, String> param) throws SQLException {
        int expectedQueryRows = Integer.parseInt(param.get("rowCount"));
        List actualNotBetweenList = betweenObj.notBetweenQueryStartEQEnd(param.get("betweenState"), param.get("testField"));
        Assert.assertEquals(actualNotBetweenList.size(), expectedQueryRows);
        Assert.assertFalse(actualNotBetweenList.contains(param.get("testValue")));
    }

    @Test(priority = 19, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????????????????????????????????????????")
    public void test20NotBetweenQueryFullRange(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryByFullRange(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 20, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between?????????????????????????????????????????????????????????")
    public void test21NotBetweenQueryInvalidDateTime(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryInvalidDateTime(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 21, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???????????????????????????????????????????????????????????????")
    public void test22NotBetweenQueryInvalidStartDate() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest22List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartDate();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 22, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???????????????????????????????????????????????????????????????")
    public void test23NotBetweenQueryInvalidEndDate() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest23List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndDate();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 23, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???????????????????????????????????????????????????????????????")
    public void test24NotBetweenQueryInvalidStartTime() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest24List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTime();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 24, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between???????????????????????????????????????????????????????????????")
    public void test25NotBetweenQueryInvalidEndTime() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest25List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTime();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 25, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????timestamp?????????????????????????????????????????????")
    public void test26NotBetweenQueryInvalidStartTimestamp1() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest26List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTimestamp1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 26, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????timestamp?????????????????????????????????????????????")
    public void test27NotBetweenQueryInvalidStartTimestamp2() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest27List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidStartTimestamp2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 27, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????timestamp?????????????????????????????????????????????")
    public void test28NotBetweenQueryInvalidEndTimestamp1() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest28List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTimestamp1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 28, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????timestamp?????????????????????????????????????????????")
    public void test29NotBetweenQueryInvalidEndTimestamp2() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest29List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryInvalidEndTimestamp2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 29, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between??????????????????????????????????????????")
    public void test30NotBetweenNoValueMatch(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryNoValueMatched(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 30, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            expectedExceptions = SQLException.class, description = "??????between?????????????????????????????????????????????????????????")
    public void test31BetweenNotSupportDateFormat(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryNotSupportDateFormat(param.get("betweenState"));
    }

    @Test(priority = 31, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            expectedExceptions = SQLException.class, description = "??????not between?????????????????????????????????????????????????????????")
    public void test32NotBetweenNotSupportDateFormat(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryNotSupportDateFormat(param.get("betweenState"));
    }

    @Test(priority = 32, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable1"},
            expectedExceptions = SQLException.class, description = "????????????????????????????????????")
    public void test33IncorrectParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.queryIncorrectParam(param.get("betweenState"));
    }

    @Test(priority = 33, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable2"},
            description = "?????????????????????Null????????????")
    public void test34BetweenNullParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.betweenQueryNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 34, enabled = true, dependsOnMethods = {"test00CreateBetweenTable2"},
            description = "not between?????????????????????????????????null?????????????????????null??????????????????")
    public void test35NotBetweenSingleNullParam1() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest35List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue1();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 35, enabled = true, dependsOnMethods = {"test00CreateBetweenTable2"},
            description = "not between?????????????????????????????????null?????????????????????null??????????????????")
    public void test36NotBetweenSingleNullParam1() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest36List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue2();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 36, enabled = true, dependsOnMethods = {"test00CreateBetweenTable2"},
            description = "not between?????????????????????????????????null?????????????????????null??????????????????")
    public void test37NotBetweenSingleNullParam3() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest37List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQuerySingleNullValue3();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 37, enabled = true, dataProvider = "yamlBetweenMethod",dependsOnMethods = {"test00CreateBetweenTable2"},
            description = "??????not between??????????????????Null????????????")
    public void test38NotBetweenBothNullParam(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.notBetweenQueryBothNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 38, enabled = true, dataProvider = "yamlBetweenMethod", dependsOnMethods = {"test00CreateBetweenTable3"},
            description = "??????????????????????????????Null???????????????")
    public void test39ColumnNull(Map<String, String> param) throws SQLException {
        Boolean actualQueryResult = betweenObj.queryColumnNullValue(param.get("betweenState"));
        System.out.println("Actual: " + actualQueryResult);

        Assert.assertFalse(actualQueryResult);
    }

    @Test(priority = 39, enabled = true, dependsOnMethods = {"test00CreateBetweenTable4"},
            description = "??????between?????????????????????null????????????")
    public void test40BetweenColumnPartNull() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest40List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.betweenQueryColumnPartNull();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 40, enabled = true, dependsOnMethods = {"test00CreateBetweenTable4"},
            description = "??????not between?????????????????????null????????????")
    public void test41NotBetweenColumnPartNull() throws SQLException {
        List<List> expectedNotBetweenList = expectedTest41List();
        System.out.println("Expected: " + expectedNotBetweenList);
        List<List> actualNotBetweenList = betweenObj.notBetweenQueryColumnPartNull();
        System.out.println("Actual: " + actualNotBetweenList);

        Assert.assertTrue(actualNotBetweenList.containsAll(expectedNotBetweenList));
        Assert.assertTrue(expectedNotBetweenList.containsAll(actualNotBetweenList));
    }

    @Test(priority = 41, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????between and??????")
    public void test42MultiBetweenQuery() throws SQLException {
        List<List> expectedBetweenList = expectedTest42List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.multiBetweenQuery();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 42, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "????????????not between and??????")
    public void test43MultiNotBetweenQuery() throws SQLException {
        List<List> expectedBetweenList = expectedTest43List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.multiNotBetweenQuery();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 43, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???not between????????????????????????")
    public void test44BetweenAndNotBetweenQueryTogether1() throws SQLException {
        List<List> expectedBetweenList = expectedTest44List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndNotBetweenQueryTogether1();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 44, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between???not between????????????????????????")
    public void test45BetweenAndNotBetweenQueryTogether2() throws SQLException {
        List<List> expectedBetweenList = expectedTest45List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenAndNotBetweenQueryTogether2();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 45, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between????????????????????????")
    public void test46BetweenQueryWithAggrFunc() throws SQLException {
        List expectedBetweenList = expectedTest46List();
        System.out.println("Expected: " + expectedBetweenList);
        List actualBetweenList = betweenObj.betweenQueryWithAggrFunc();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 46, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between????????????????????????")
    public void test47NotBetweenQueryWithAggrFunc() throws SQLException {
        List expectedBetweenList = expectedTest47List();
        System.out.println("Expected: " + expectedBetweenList);
        List actualBetweenList = betweenObj.notBetweenQueryWithAggrFunc();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList, expectedBetweenList);
    }

    @Test(priority = 47, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????between??????????????????")
    public void test48BetweenQueryWithAggrFunc() throws SQLException {
        List<List> expectedBetweenList = expectedTest48List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenQueryWithGroup();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 48, enabled = true, dependsOnMethods = {"test00CreateBetweenTable1"},
            description = "??????not between??????????????????")
    public void test49BetweenQueryWithAggrFunc() throws SQLException {
        List<List> expectedBetweenList = expectedTest49List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenQueryWithGroup();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertEquals(actualBetweenList,expectedBetweenList);
    }

    @Test(priority = 49, enabled = true, dependsOnMethods = {"test00CreateBetweenTable5"},
            description = "??????between?????????update???????????????,???????????????")
    public void test50BetweenInCharUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest50List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateCharState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 50, enabled = true, dependsOnMethods = {"test50BetweenInCharUpdateState"},
            description = "??????between?????????update???????????????,???????????????")
    public void test51BetweenInNumUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest51List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateNumState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 51, enabled = true, dependsOnMethods = {"test51BetweenInNumUpdateState"},
            description = "??????between?????????update???????????????,?????????????????????")
    public void test52BetweenInDateTimeUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest52List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateDateTimeState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 52, enabled = true, dependsOnMethods = {"test52BetweenInDateTimeUpdateState"},
            description = "??????between?????????update???????????????,???????????????")
    public void test53BetweenInDateTimeUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest53List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInUpdateBooleanState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 53, enabled = true, dependsOnMethods = {"test53BetweenInDateTimeUpdateState"},
            description = "??????not between?????????update???????????????,???????????????")
    public void test54NotBetweenInCharUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest54List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateCharState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 54, enabled = true, dependsOnMethods = {"test54NotBetweenInCharUpdateState"},
            description = "??????not between?????????update???????????????,???????????????")
    public void test55NotBetweenInNumUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest55List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateNumState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 55, enabled = true, dependsOnMethods = {"test55NotBetweenInNumUpdateState"},
            description = "??????not between?????????update???????????????,?????????????????????")
    public void test56NotBetweenInDateTimeUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest56List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateDateTimeState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 56, enabled = true, dependsOnMethods = {"test56NotBetweenInDateTimeUpdateState"},
            description = "??????not between?????????update???????????????,???????????????")
    public void test57NotBetweenInDateTimeUpdateState() throws SQLException {
        List<List> expectedBetweenList = expectedTest57List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInUpdateBooleanState();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 57, enabled = true, dependsOnMethods = {"test57NotBetweenInDateTimeUpdateState"},
            description = "??????between???????????????????????????")
    public void test58BetweenUpdateEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenUpdateEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 58, enabled = true, dependsOnMethods = {"test58BetweenUpdateEmpSet"},
            description = "??????not between???????????????????????????")
    public void test59BetweenUpdateEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenUpdateEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 59, enabled = true, dependsOnMethods = {"test59BetweenUpdateEmpSet"},
            description = "??????between?????????????????????")
    public void test60BetweenInDeleteNotEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenDeleteNotEmpSet();
        Assert.assertEquals(actualEffectRows, 5);

        List<List> expectedBetweenList = expectedTest60List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualAfterBetweenDeleteList = betweenObj.betweenQueryAfterDelete();
        System.out.println("Actual: " + actualAfterBetweenDeleteList);

        Assert.assertTrue(actualAfterBetweenDeleteList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualAfterBetweenDeleteList));
    }

    @Test(priority = 60, enabled = true, dependsOnMethods = {"test60BetweenInDeleteNotEmpSet"},
            description = "??????not between?????????????????????")
    public void test61NotBetweenInDeleteNotEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenDeleteNotEmpSet();
        Assert.assertEquals(actualEffectRows, 11);

        List<List> expectedBetweenList = expectedTest61List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualAfterNotBetweenDeleteList = betweenObj.notBetweenQueryAfterDelete();
        System.out.println("Actual: " + actualAfterNotBetweenDeleteList);

        Assert.assertTrue(actualAfterNotBetweenDeleteList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualAfterNotBetweenDeleteList));
    }

    @Test(priority = 61, enabled = true, dependsOnMethods = {"test61NotBetweenInDeleteNotEmpSet"},
            description = "??????between???????????????????????????")
    public void test62BetweenDeleteEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.betweenDeleteEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 62, enabled = true, dependsOnMethods = {"test61NotBetweenInDeleteNotEmpSet"},
            description = "??????not between???????????????????????????")
    public void test63NotBetweenDeleteEmpSet() throws SQLException {
        int actualEffectRows = betweenObj.notBetweenDeleteEmpSet();
        System.out.println("Actual: " + actualEffectRows);

        Assert.assertEquals(actualEffectRows, 0);
    }

    @Test(priority = 63, enabled = true, dependsOnMethods = {"test00CreateBetweenTable67"},
            description = "??????not between???????????????????????????")
    public void test64BetweenInInnerJoin() throws SQLException {
        List<List> expectedBetweenList = expectedTest64List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.betweenInInnerJoin();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }

    @Test(priority = 64, enabled = true, dependsOnMethods = {"test00CreateBetweenTable67"},
            description = "??????not between???????????????????????????")
    public void test65NotBetweenInInnerJoin() throws SQLException {
        List<List> expectedBetweenList = expectedTest65List();
        System.out.println("Expected: " + expectedBetweenList);
        List<List> actualBetweenList = betweenObj.notBetweenInInnerJoin();
        System.out.println("Actual: " + actualBetweenList);

        Assert.assertTrue(actualBetweenList.containsAll(expectedBetweenList));
        Assert.assertTrue(expectedBetweenList.containsAll(actualBetweenList));
    }


    @AfterClass(alwaysRun = true, description = "???????????????????????????????????????????????????")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try {
            tearDownStatement = BetweenState.connection.createStatement();
            tearDownStatement.execute("delete from betweenTest");
            tearDownStatement.execute("drop table betweenTest");
            tearDownStatement.execute("delete from betweenTest2");
            tearDownStatement.execute("drop table betweenTest2");
            tearDownStatement.execute("delete from betweenTest3");
            tearDownStatement.execute("drop table betweenTest3");
            tearDownStatement.execute("delete from betweenTest4");
            tearDownStatement.execute("drop table betweenTest4");
            tearDownStatement.execute("delete from betweenTest5");
            tearDownStatement.execute("drop table betweenTest5");
            tearDownStatement.execute("delete from between_employees");
            tearDownStatement.execute("drop table between_employees");
            tearDownStatement.execute("delete from between_job_grades");
            tearDownStatement.execute("drop table between_job_grades");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (tearDownStatement != null) {
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if (BetweenState.connection != null) {
                    BetweenState.connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
