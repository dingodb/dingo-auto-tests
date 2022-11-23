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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StrTo2DList {
    /**
     * @param constructStr e.g. 5,awJDs,2010-10-01;19,Adidas,2010-10-01
     * @return List<List>
     */
    public List<List> construct2DList(String constructStr, String listSplit, String strSplit){
        List<List> constructList = new ArrayList<List>();
        String[] originArray = constructStr.split(listSplit);

        for(int i=0; i < originArray.length; i++) {
            constructList.add(new ArrayList(Arrays.asList(originArray[i].split(strSplit))));
        }

        return constructList;
    }

    public List construct1DList(String constructStr, String splitCha){
        List constructList = new ArrayList();
        String[] originArray = constructStr.split(splitCha);
        constructList = Arrays.asList(originArray);

        return constructList;
    }

    public List<List> construct2DListIncludeBlank(String constructStr, String listSplit, String strSplit){
        List<List> constructList = new ArrayList<List>();
        String[] originArray = constructStr.split(listSplit,-1);

        for(int i=0; i < originArray.length; i++) {
            constructList.add(new ArrayList(Arrays.asList(originArray[i].split(strSplit,-1))));
        }

        return constructList;
    }

    public List construct1DListIncludeBlank(String constructStr, String splitCha){
        List constructList = new ArrayList();
        String[] originArray = constructStr.split(splitCha,-1);
        constructList = Arrays.asList(originArray);

        return constructList;
    }
}
