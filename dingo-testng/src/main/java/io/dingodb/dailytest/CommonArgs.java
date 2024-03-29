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

package io.dingodb.dailytest;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonArgs {

    public static String getDefaultDingoClusterIP() {
        if (System.getenv().containsKey("ConnectIP")) {
            return System.getenv("ConnectIP");
        }
        return  "172.20.3.27";
    }

    public static String getDefaultConnectUser() {
        if (System.getenv().containsKey("ConnectUser")) {
            return System.getenv("ConnectUser");
        }
        return  null;
    }

    public static String getDefaultConnectPass() {
        if (System.getenv().containsKey("ConnectPass")) {
            return System.getenv("ConnectPass");
        }
        return  null;
    }

    public static String getCurDateStr() {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateNowStr = simpleDateFormat.format(date);
        return dateNowStr;
    }
}
