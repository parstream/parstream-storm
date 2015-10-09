/**
 * Copyright 2015 ParStream GmbH
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
package com.parstream.driver.mapping;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;

import org.junit.Test;

import com.parstream.driver.datatypes.AutoCastException;
import com.parstream.driver.datatypes.OutOfRangeException;
import com.parstream.driver.datatypes.PsInt16;
import com.parstream.driver.datatypes.PsUint16;
import com.parstream.driver.datatypes.PsValue;
import com.parstream.driver.datatypes.PsVarstring;
import com.parstream.driver.mapping.ArraySource;
import com.parstream.driver.mapping.DataSource;
import com.parstream.driver.mapping.RowCreator;

public class RowCreatorTest {

    @Test
    public void createRow() throws AutoCastException, OutOfRangeException {
        // create mapping based on target column array
        DataSource<Object[]>[] mapping = new ArraySource[4];
        mapping[0] = new ArraySource(0);
        mapping[1] = new ArraySource(1);
        mapping[2] = new ArraySource();
        mapping[3] = new ArraySource(2);

        // create target field list from column info
        LinkedHashMap<String, PsValue> rowTemplate = new LinkedHashMap<>();
        rowTemplate.put("id", new PsInt16());
        rowTemplate.put("name", new PsVarstring());
        rowTemplate.put("extid", new PsUint16());
        rowTemplate.put("count", new PsUint16());

        // create input data
        Object input[] = { (short) 21, "hello", 42 };

        // create row creator
        RowCreator<Object[]> mapper = new RowCreator<Object[]>(rowTemplate);
        assertEquals(rowTemplate.size(), mapper.getRowSize());
        assertEquals(rowTemplate.keySet(), mapper.getColumnNames());

        // convert input into object row array
        Object[] row_data = mapper.createRow(input, mapping);
        Object output[] = { (short) 21, "hello", null, 42 };
        assertArrayEquals(output, row_data);
    }
}
