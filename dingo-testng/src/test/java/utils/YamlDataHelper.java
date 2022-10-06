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
                String nameValue = String.valueOf(nameMapEntry.getValue());
//                String nameValue = nameMapEntry.getValue();
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
            case "test13Unix_TimeStampNumArg": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "unix_timestamp_numArg"));
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
            case "test14Date_FormatMissingFormatArg": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "date_format_missingFormatArg"));
                break;
            }
            case "test22Time_FormatStrArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "time_format_str"));
                break;
            }
            case "test22Time_FormatNumArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "time_format_num"));
                break;
            }
            case "test22Time_FormatArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "time_format_funcArg"));
                break;
            }
            case "test22Time_FormatMissingFormatArg": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "time_format_missingFormatArg"));
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
            case "test15DateDiffEmptyNullState": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "datediff_emptyNull"));
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
            case "test16VariousFormatTimestampInsert": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "insert_timestamp"));
                break;
            }
            case "test17FuncConcatStr": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "funcConcatStr"));
                break;
            }
            case "test21InsertWithFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "create_table_funcs"));
                break;
            }
            case "test27Timestamp_FormatFuncArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "timestamp_format_funcArg"));
                break;
            }
            case "test27Timestamp_FormatStrArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "timestamp_format_str"));
                break;
            }
            case "test27Timestamp_FormatNumArgFunc": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "timestamp_format_num"));
                break;
            }
            case "test27Timestamp_FormatMissingFormatArg": {
                yamlList = getYamlList(iniReader.getValue("DateTimeYaml", "timestamp_format_missingFormatArg"));
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
    public Object[][] yamlNegativeDateTimeMethod(Method method) {
        List<Map<String, String>> yamlNegativeDateTimeList = null;
        switch (method.getName()) {
            case "test18InsertNegativeDate": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "insert_negative_date"));
                break;
            }

            case "test19InsertNegativeTime": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "insert_negative_time"));
                break;
            }

            case "test20InsertNegativeTimeStamp": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "insert_negative_timestamp"));
                break;
            }

            case "test13Unix_TimeStampNegativeDate": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "unix_timestamp_negative_date"));
                break;
            }

            case "test13Unix_TimeStampNegativeNum": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "unix_timestamp_negative_num"));
                break;
            }

            case "test14Date_FormatNegativeDate": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "date_format_negative"));
                break;
            }

            case "test14Date_FormatMissingDateArg": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "date_format_negative_missingArg"));
                break;
            }

            case "test22Time_FormatNegativeTimeStr": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "time_format_negative"));
                break;
            }

            case "test22Time_FormatNegativeTimeNum": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "time_format_negative_num"));
                break;
            }

            case "test22Time_FormatNegativeArgFunc": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "time_format_negative_funcArg"));
                break;
            }

            case "test22Time_FormatMissingArg": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "time_format_negative_missingArg"));
                break;
            }

            case "test15DateDiffNegativeDate": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "datediff_negative"));
                break;
            }

            case "test15DateDiffWrongArg": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "datediff_negatieve_wrongArg"));
                break;
            }

            case "test27Timestamp_FormatNegativeTimestamp": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "timestamp_format_negative"));
                break;
            }

            case "test27Timestamp_FormatWrongArg": {
                yamlNegativeDateTimeList = getYamlList(iniReader.getValue("DateTimeNegativeYaml", "timestamp_format_wrongArg"));
                break;
            }
        }
        Object[][] files = new Object[yamlNegativeDateTimeList.size()][];
        for (int i = 0; i < yamlNegativeDateTimeList.size(); i++) {
            files[i] = new Object[]{yamlNegativeDateTimeList.get(i)};
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

    @DataProvider
    public Object[][] yamlStrFuncMethod(Method method) {
        List<Map<String, String>> yamlStrFuncList = null;
        switch (method.getName()) {
            case "test16Char_LengthStr": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "char_length"));
                break;
            }
            case "test17Char_LengthNonStr": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "char_lengthNonStr"));
                break;
            }

            case "testFormatCase092": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "formatYaml"));
                break;
            }

            case "testLocateCase096": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "locate_str"));
                break;
            }
            case "testLocateCase098": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "locate_int_str"));
                break;
            }
            case "testLocateCase099": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "locate_int_int"));
                break;
            }

            case "testLocateCase101": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "locate_str_int"));
                break;
            }

            case "testLowerCase119": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "lowerCase"));
                break;
            }

            case "testUpperCase125": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "upperCase"));
                break;
            }

            case "testLeftCase127": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "leftStr"));
                break;
            }

            case "testLeftCase130": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "leftNum"));
                break;
            }

            case "testRightCase138": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "rightStr"));
                break;
            }

            case "testRightCase141": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "rightNum"));
                break;
            }

            case "testRepeatCase148": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "repeatStr"));
                break;
            }

            case "testRepeatCase157": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "repeatNum"));
                break;
            }

            case "testReplaceCase163": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "replaceStr"));
                break;
            }

            case "testReplaceCase170": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "replaceNum"));
                break;
            }

            case "testReplaceCase173": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "replaceStrNum"));
                break;
            }

            case "testReplaceCase175": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "replaceNumStr"));
                break;
            }

            case "testTrimCase181": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimStr"));
                break;
            }

            case "testTrimCase182": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimLeadingStr"));
                break;
            }

            case "testTrimCase183": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimTrailingStr"));
                break;
            }

            case "testTrimCase184": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimBothStr"));
                break;
            }

            case "testTrimCase185": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimLeadingXStr"));
                break;
            }

            case "testTrimCase186": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimTrailingXStr"));
                break;
            }

            case "testTrimCase187": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimBothXStr"));
                break;
            }

            case "testTrimCase188": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimBothXStr"));
                break;
            }

            case "testTrimCase192": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimNum"));
                break;
            }

            case "testLtrimCase194": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimLeadingStr"));
                break;
            }

            case "testRtrimCase196": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimTrailingStr"));
                break;
            }

            case "testTrimCase195": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "trimException"));
                break;
            }

            case "testMidCase201": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midStr"));
                break;
            }

            case "testMidCase207": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midStrException"));
                break;
            }

            case "testMidCase210_1": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midNum"));
                break;
            }

            case "testMidCase210_2": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midNumException"));
                break;
            }

            case "testMidCase221": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midParamException"));
                break;
            }

            case "testMidCase212": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "midIgnoreLength"));
                break;
            }

            case "testSubStringCase222": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringStr"));
                break;
            }

            case "testSubStringCase227": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringStrException"));
                break;
            }

            case "testSubStringCase231_1": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringNum"));
                break;
            }

            case "testSubStringCase231_2": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringNumException"));
                break;
            }

            case "testSubStringCase238": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringParamException"));
                break;
            }

            case "testSubStringCase241": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "subStringFromForLength"));
                break;
            }

            case "testReverseCase248": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "reverseStr"));
                break;
            }

            case "testReverseCase249": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "reverseNum"));
                break;
            }

            case "testReverseCase255": {
                yamlStrFuncList = getYamlList(iniReader.getValue("strFuncYaml", "reverseException"));
                break;
            }

        }
        Object[][] files = new Object[yamlStrFuncList.size()][];
        for (int i = 0; i < yamlStrFuncList.size(); i++) {
            files[i] = new Object[]{yamlStrFuncList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] yamlBetweenMethod(Method method) {
        List<Map<String, String>> yamlBetweenList = null;
        switch (method.getName()) {
            case "test08BetweenStartGTEnd": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_StartGTEnd"));
                break;
            }

            case "test09BetweenQueryFullRange": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_FullRange"));
                break;
            }

            case "test10BetweenQueryInvalidDateTime": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_InvalidDateTime"));
                break;
            }

            case "test11BetweenNoValueMatch": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_NoValueMatch"));
                break;
            }

            case "test19NotBetweenStartGTEnd": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_StartGTEnd"));
                break;
            }

            case "test20NotBetweenQueryFullRange": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_FullRange"));
                break;
            }

            case "test21NotBetweenQueryInvalidDateTime": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_InvalidDateTime"));
                break;
            }

            case "test30NotBetweenNoValueMatch": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_NoValueMatch"));
                break;
            }

            case "test31BetweenSupportOtherDateFormat": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_SupportOtherDateFormat"));
                break;
            }

            case "test32NotBetweenSupportOtherDateFormat": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_SupportOtherDateFormat"));
                break;
            }

            case "test33IncorrectParam": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "query_incorrectParam"));
                break;
            }

            case "test34BetweenNullParam": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_NullValue"));
                break;
            }

            case "test38NotBetweenBothNullParam": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_NullValue"));
                break;
            }

            case "test39ColumnNull": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "query_ColumnNull"));
                break;
            }

            case "test08BetweenStartEQEnd": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_StartEQEnd"));
                break;
            }

            case "test19NotBetweenStartEQEnd": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "not_between_StartEQEnd"));
                break;
            }

            case "test70QueryUsingVarcharPrimaryKey": {
                yamlBetweenList = getYamlList(iniReader.getValue("BetweenAndYaml", "between_varcharPrimaryKey"));
                break;
            }

        }
        Object[][] files = new Object[yamlBetweenList.size()][];
        for (int i = 0; i < yamlBetweenList.size(); i++) {
            files[i] = new Object[]{yamlBetweenList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] yamlNumericFuncMethod(Method method) {
        List<Map<String, String>> yamlNumericFuncList = null;
        switch (method.getName()) {
            case "test01PowPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_positive"));
                break;
            }

            case "test01PowRangeGetDouble": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_range_getDouble"));
                break;
            }

            case "test01PowRangeGetBigDecimal": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_range_getBigDecimal"));
                break;
            }

            case "test02PowXStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_x_str"));
                break;
            }

            case "test03PowYDecimal": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_y_decimal"));
                break;
            }

            case "test04PowYStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_y_str"));
                break;
            }

            case "test05PowWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "pow_wrong_state"));
                break;
            }

            case "test06RoundPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_positive"));
                break;
            }

            case "test06RoundIntRange": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_range_int"));
                break;
            }

            case "test07RoundYDecimal": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_y_decimal"));
                break;
            }

            case "test08RoundXStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_x_str"));
                break;
            }

            case "test09RoundYStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_y_str"));
                break;
            }

            case "test10RoundWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_wrong_state"));
                break;
            }

            case "test11RoundXYStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_xy_str"));
                break;
            }

            case "test12RoundXOnly": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "round_x_only"));
                break;
            }

            case "test13CeilingPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "ceiling_positive"));
                break;
            }

            case "test13CeilingIntRange": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "ceiling_range_int"));
                break;
            }

            case "test14CeilingWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "ceiling_wrong_state"));
                break;
            }

            case "test15CeilingStrArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "ceiling_str"));
                break;
            }

            case "test16CeilFunc": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "ceil_func"));
                break;
            }

            case "test17FloorPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "floor_positive"));
                break;
            }

            case "test17FloorIntRange": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "floor_range_int"));
                break;
            }

            case "test18FloorWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "floor_wrong_state"));
                break;
            }

            case "test19FloorStrArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "floor_str"));
                break;
            }

            case "test20ABSPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "abs_positive"));
                break;
            }

            case "test20ABSIntRange": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "abs_range_int"));
                break;
            }

            case "test21ABSWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "abs_wrong_state"));
                break;
            }

            case "test22ABSStrArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "abs_str"));
                break;
            }

            case "test23ModPositiveArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "mod_positive"));
                break;
            }

            case "test24ModWrongArg": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "mod_wrong_state"));
                break;
            }

            case "test25ModXStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "mod_x_str"));
                break;
            }

            case "test26ModYStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "mod_y_str"));
                break;
            }

            case "test27ModXYStr": {
                yamlNumericFuncList = getYamlList(iniReader.getValue("NumericFuncsYaml", "mod_xy_str"));
                break;
            }

        }
        Object[][] files = new Object[yamlNumericFuncList.size()][];
        for (int i = 0; i < yamlNumericFuncList.size(); i++) {
            files[i] = new Object[]{yamlNumericFuncList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] yamlSQLFuncMethod(Method method) {
        List<Map<String, String>> yamlSQLFuncList = null;
        switch (method.getName()) {
            case "testQueryDateTimeEQCondition": {
                yamlSQLFuncList = getYamlList(iniReader.getValue("SQLFuncsYaml", "query_datetime_eq"));
                break;
            }

            case "testQueryTypeNECondition": {
                yamlSQLFuncList = getYamlList(iniReader.getValue("SQLFuncsYaml", "query_type_ne"));
                break;
            }

            case "testQueryDateTimeInRangeCondition": {
                yamlSQLFuncList = getYamlList(iniReader.getValue("SQLFuncsYaml", "query_datetime_in"));
                break;
            }

            case "testQueryDateTimeNotInRangeCondition": {
                yamlSQLFuncList = getYamlList(iniReader.getValue("SQLFuncsYaml", "query_datetime_not_in"));
                break;
            }

            case "testQueryTypeNotInRangeCondition": {
                yamlSQLFuncList = getYamlList(iniReader.getValue("SQLFuncsYaml", "query_type_not_in"));
                break;
            }
        }
        Object[][] files = new Object[yamlSQLFuncList.size()][];
        for (int i = 0; i < yamlSQLFuncList.size(); i++) {
            files[i] = new Object[]{yamlSQLFuncList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] arrayFieldMethod(Method method) {
        List<Map<String, String>> yamlArrayList = null;
        switch (method.getName()) {
            case "test01TableCreateWithArrayField": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayfield"));
                break;
            }

            case "test02InsertArrayValues": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayvalues"));
                break;
            }

            case "test02QueryArrayData": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "querydata"));
                break;
            }

            case "test03TableCreateWithArrayFieldDefaultValue": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayfield_withdefaultvalue"));
                break;
            }

            case "test04InsertValuesWithArrayField": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayfield_withdefaultvalue"));
                break;
            }

            case "test05QueryWithArrayColNull": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayfield_null"));
                break;
            }

            case "test05InsertWithArrayElemNull": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "arrayelem_null"));
                break;
            }

            case "test06InsertMixedAndIllegalElement": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "array_exception"));
                break;
            }

            case "test07InsertSingleElementTypeSuccess": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "insertSingleElemType"));
                break;
            }

            case "test17QueryInMidAndFirst": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "query_mid_first"));
                break;
            }

            case "test21RangeQuery": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "query_inrange"));
                break;
            }

            case "test22DeleteRecordsWithArrayNull": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "array_delete"));
                break;
            }

            case "test25UpdateArrayAndQuery": {
                yamlArrayList = getYamlList(iniReader.getValue("ArrayFieldYaml", "array_update"));
                break;
            }

        }
        Object[][] files = new Object[yamlArrayList.size()][];
        for (int i = 0; i < yamlArrayList.size(); i++) {
            files[i] = new Object[]{yamlArrayList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] multisetFieldMethod(Method method) {
        List<Map<String, String>> yamlMultisetList = null;
        switch (method.getName()) {
            case "test01TableCreateWithMultisetField": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetfield"));
                break;
            }

            case "test02InsertMultisetValues": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetfield"));
                break;
            }

            case "test03QueryMultisetData": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetfield"));
                break;
            }

            case "test04InsertMultisetColumnNull": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetfield_null"));
                break;
            }

            case "test05MultisetValueNotSupport": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetvalue_exception"));
                break;
            }

            case "test06MixValuesSupport": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multisetmixvalues"));
                break;
            }

            case "test10UpdateMultisetAndQuery": {
                yamlMultisetList = getYamlList(iniReader.getValue("MultisetFieldYaml", "multiset_update"));
                break;
            }

        }
        Object[][] files = new Object[yamlMultisetList.size()][];
        for (int i = 0; i < yamlMultisetList.size(); i++) {
            files[i] = new Object[]{yamlMultisetList.get(i)};
        }
        return files;
    }



    @DataProvider
    public Object[][] mapFieldMethod(Method method) {
        List<Map<String, String>> yamlMapList = null;
        switch (method.getName()) {
            case "test07InsertVarcharAndNumKV": {
                yamlMapList = getYamlList(iniReader.getValue("MapFieldYaml", "map1_exception"));
                break;
            }

            case "test17VerifyIntValueRangeNotSupport": {
                yamlMapList = getYamlList(iniReader.getValue("MapFieldYaml", "map2_exception"));
                break;
            }
        }
        Object[][] files = new Object[yamlMapList.size()][];
        for (int i = 0; i < yamlMapList.size(); i++) {
            files[i] = new Object[]{yamlMapList.get(i)};
        }
        return files;
    }

    @DataProvider
    public Object[][] createTableMethod(Method method) {
        List<Map<String, String>> yamlCreateTableList = null;
        switch (method.getName()) {
            case "test08TableCreateWithMultiPrimaryKey": {
                yamlCreateTableList = getYamlList(iniReader.getValue("createTableYaml", "mpkey_state"));
                break;
            }

            case "test09InsertValuesWithMultiPrimaryKey": {
                yamlCreateTableList = getYamlList(iniReader.getValue("createTableYaml", "mpkey_insert_value"));
                break;
            }

        }
        Object[][] files = new Object[yamlCreateTableList.size()][];
        for (int i = 0; i < yamlCreateTableList.size(); i++) {
            files[i] = new Object[]{yamlCreateTableList.get(i)};
        }
        return files;
    }

}
