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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

public class IniReader {
    protected HashMap sections = new HashMap();
    private transient String currentSection;
    private transient Properties current;

    /**
     * 构造函数
     */
    public IniReader(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        read(reader);
        reader.close();
    }

    /**
     * 读取文件
     */
    public void read(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null){
            parseLine(line);
        }
    }

    /**
     * 解析配置文件行
     */
    protected void parseLine(String line){
        line = line.trim();
        if (line.matches("\\[.*]")) {
            //取出正则表达式匹配到的内容中的第一个条件组中的内容
            currentSection = line.replaceFirst("\\[(.*)]", "$1");
            //创建一个Properties对象
            current = new Properties();
            //将currentSection和current以键值对的形式存放在map集合中
            sections.put(currentSection, current);
        }else if (line.matches(".*=.*")) {
            if (current != null) {
                int i = line.indexOf("=");
                String name = line.substring(0, i).trim();
                String value = line.substring(i + 1).trim();
                current.setProperty(name, value);
            }

        }
    }

    /**
     * 根据提供的键获取值
     */
    public String getValue(String section, String key) {
        Properties p = (Properties) sections.get(section);
        if (p == null) {
            return null;
        }
        String value = p.getProperty(key);
        return value;
    }

}
