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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Random;

public class GetRandomValue {
    private static Random rd = new Random();

    //获取最大值为max以内的随机整数
    public static int getRandInt(int max) {
        return rd.nextInt(max) + 1;
    }

    //获取max以内的随机浮点数
    public static double getRandDouble(int min, int max) {
        return min + ((max -min) * rd.nextDouble());
    }

    //获取指定长度的字母和数字组成的字符串
    public static String getRandStr(int strLeng) {
        return RandomStringUtils.randomAlphanumeric(strLeng);
    }

    //格式化浮点数值字符串,保留指定位数
    public static String formateRate(String rateStr, int decimalNum) {
        if (rateStr.indexOf(".") != -1) {
            //获取小数点位置
            int num = 0;
            num = rateStr.indexOf(".");

            String pointAfter = rateStr.substring(0, num + 1);
            String afterData = rateStr.replace(pointAfter, "");

            return rateStr.substring(0, num) + "." + afterData.substring(0, decimalNum);
        } else {
            return rateStr + "." + StringUtils.repeat("0", decimalNum);
        }
    }

}
