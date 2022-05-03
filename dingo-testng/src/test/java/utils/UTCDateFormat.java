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

/**
 * @Status: Deprecated
 */

package utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class UTCDateFormat {
    private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    public String getUTCDateStr() {
        StringBuffer UTCDateBuffer = new StringBuffer();
        // 取得本地时间
        Calendar cal = Calendar.getInstance();
        // 取得时间偏移量
        int zoneOffset = cal.get(Calendar.ZONE_OFFSET);
        // 取得夏令时差
        int dstOffset = cal.get(Calendar.DST_OFFSET);
        // 从本地时间里扣除这些差量，即可以取得UTC时间
        cal.add(Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        int year = cal.get(Calendar.YEAR);
        int month =  cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
//        int hour = cal.get(Calendar.HOUR_OF_DAY);
//        int minute = cal.get(Calendar.MINUTE);

        UTCDateBuffer.append(year).append("-").append(month).append("-").append(day);
        try{
            format.parse(UTCDateBuffer.toString());
            return UTCDateBuffer.toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String formatUTCDate(String inputUTCDate){
        java.util.Date UTCDate = null;
        String UTCDateStr = null;
        try{
            UTCDate = format.parse(inputUTCDate);
            format.setTimeZone(TimeZone.getTimeZone("GMT+16"));
            UTCDateStr = format.format(UTCDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return UTCDateStr;
    }
}
