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

package utils;

import org.testng.annotations.DataProvider;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlDataHelper{
    public static IniReader iniReader;

    static {
        try {
            iniReader = new IniReader("src/test/resources/ini/my.ini");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String>> getYamlList(String yamlfile) {
        List<Map<String, String>> list = new ArrayList();
        Map<String, Map<String, String>> map = readYamlUtil(yamlfile);
        for(Map.Entry<String, Map<String,String>> me : map.entrySet()){
            Map<String, String> numNameMapValue = me.getValue();
            Map<String, String> tmp = new HashMap<>();
            for (Map.Entry<String, String> nameMapEntry : numNameMapValue.entrySet()){
                String numKey = nameMapEntry.getKey();
                String nameValue = nameMapEntry.getValue();
                tmp.put(numKey, nameValue);
            }
            list.add(tmp);
        }
        return list;
    }

    public static Map<String, Map<String, String>> readYamlUtil(String fileName) {
        Map<String, Map<String, String>> map = null;
        try{
            Yaml yaml = new Yaml();
            URL url = YamlDataHelper.class.getClassLoader().getResource(fileName);
            if(url != null){
                // 获取yaml文件中的配置数据，然后转换为Map
                map = yaml.load(new FileInputStream(url.getFile()));
                return map;

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return map;
    }

    @DataProvider
    public Object[][] yamlDataMethod(Method method) {
        List<Map<String, String>> yamlList = null;
        switch (method.getName()) {
            case "test12From_UnixTimeFunc_TimestampInput": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "from_unixtime"));
                break;
            }
            case "test13Unix_TimeStampFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "unix_timestamp"));
                break;
            }
            case "test13Unix_TimeStampFuncArg": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "unix_timestamp_funcArg"));
                break;
            }
            case "test14Date_FormatStrArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "date_format_str"));
                break;
            }
            case "test14Date_FormatNumArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "date_format_num"));
                break;
            }
            case "test14Date_FormatFuncArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "date_format_funcArg"));
                break;
            }
            case "test15DateDiffStrArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff_str"));
                break;
            }
            case "test15DateDiffNumArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff_num"));
                break;
            }
            case "test15DateDiffFuncArg1Func": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff_funcArg"));
                break;
            }
            case "test15DateDiffFuncArg2Func": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff_funcArg"));
                break;
            }
            case "test16VariousFormatDateInsert": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "insert_date"));
                break;
            }
            case "test16VariousFormatTimeInsert": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "insert_time"));
                break;
            }
            case "test17FuncConcatStr": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "funcConcatStr"));
                break;
            }
        }
        Object[][] files = new Object[yamlList.size()][];
        for (int i = 0; i < yamlList.size(); i++) {
            files[i] = new Object[]{yamlList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] yamlBooleanMethod(Method method) {
        List<Map<String, String>> yamlBooleanList = null;
        switch (method.getName()) {
            case "test09InsertStrValue": {
                yamlBooleanList = getYamlList(iniReader.getValue("BooleanField", "strValue"));
                break;
            }
            case "test10InsertWrongValue": {
                yamlBooleanList = getYamlList(iniReader.getValue("BooleanField", "wrongValue"));
                break;
            }
            case "test12IntegerValueQuery": {
                yamlBooleanList = getYamlList(iniReader.getValue("BooleanField", "intValue"));
                break;
            }
        }
        Object[][] files = new Object[yamlBooleanList.size()][];
        for (int i = 0; i < yamlBooleanList.size(); i++) {
            files[i] = new Object[]{yamlBooleanList.get(i)};
        }
        return files;

    }
}
