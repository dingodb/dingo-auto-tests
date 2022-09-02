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

import java.math.BigDecimal;
import java.sql.Date;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    //格式化浮点数,保留指定位数,方法一
    public static double formatDoubleDecimal1(double formatNum, int decimalNum) {
        BigDecimal bg = new BigDecimal(formatNum);
        double f1 = bg.setScale(decimalNum, BigDecimal.ROUND_HALF_UP).doubleValue();
        return  f1;
    }

    //格式化浮点数,保留指定位数,方法二
    public static double formatDoubleDecimal2(double formatNum) {
        DecimalFormat df = new DecimalFormat("#0.00");
        double f2 = Double.parseDouble(df.format(formatNum));
        return f2;
    }

    //格式化浮点数,保留指定位数,方法三
    public static double formatDoubleDecimal3(double formatNum) {
        double f3 = Double.parseDouble(String.format("%.2f", formatNum));
        return f3;
    }

    //格式化字符串为小数数值,保留指定位数,方法四
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

    /**
     *
     * @param patternFormat, e.g. "yyyy-MM-dd"
     * @param startDateStr, e.g. "1990-01-01 00:00:00"
     * @param endDateStr, e.g. "2022-09-01 23:59:59"
     * @return Date
     * @throws ParseException
     */
    public static Date getRandomDate(String patternFormat, String startDateStr, String endDateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(patternFormat);
        long start = sdf.parse(startDateStr).getTime();
        long end = sdf.parse(endDateStr).getTime();
        long randomDate = nextLong(start, end);
        return Date.valueOf(sdf.format(randomDate));
    }

    public static long nextLong(long start, long end) {
        Random random = new Random();
        return start + (long) (random.nextDouble() * (end - start + 1));
    }

}
