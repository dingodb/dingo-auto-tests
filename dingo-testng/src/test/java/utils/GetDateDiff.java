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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDateDiff {
    public static long getDiffDate(String inputDate) throws ParseException {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String curDate = simpleDateFormat.format(date);
        Date startDate = simpleDateFormat.parse(curDate);
        Date endDate = simpleDateFormat.parse(inputDate);
        long diffDate = (startDate.getTime() - endDate.getTime())/(1000*60*60*24);
        return diffDate;
    }
}
