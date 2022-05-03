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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JDK8DateTime {

    // 格式化当前日期为 yyyy-MM-dd
    public static String formatCurDate() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime localDateTime = LocalDateTime.now();
        String formatCurDateStr = dateTimeFormatter.format(localDateTime);
        return formatCurDateStr;
    }

    // 格式化当前时间为 HH:mm
    public static String formatCurTime() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm");
        LocalDateTime localDateTime = LocalDateTime.now();
        String formatCurTimeStr = dateTimeFormatter.format(localDateTime);
        return formatCurTimeStr;
    }

    // 格式化当前日期时间为 yyyy-MM-dd HH:mm
    public static String formatNow() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime localDateTime = LocalDateTime.now();
        String formatDateTimeStr = dateTimeFormatter.format(localDateTime);
        return formatDateTimeStr;
    }

}
