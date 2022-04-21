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

//    public static String datediffFuncFile = "testdata/datetime/datediff/datediff.yaml";

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
            case "test10Unix_TimeStampFunc":{
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "unix_timestamp"));
                break;
            }
            case "test11Date_FormatFunc":{
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "date_format"));
                break;
            }
            case "test12DateDiffFunc":{
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff"));
                break;
            }
        }
        Object[][] files = new Object[yamlList.size()][];
        for (int i = 0; i < yamlList.size(); i++) {
            files[i] = new Object[]{yamlList.get(i)};
        }
        return files;
    }
}
