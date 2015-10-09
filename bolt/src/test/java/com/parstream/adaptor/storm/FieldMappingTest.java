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
package com.parstream.adaptor.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.junit.Test;

import com.parstream.adaptor.storm.mapping.FieldMapper;
import com.parstream.adaptor.storm.mapping.FieldSource;

import backtype.storm.tuple.Tuple;

public class FieldMappingTest {

    @Test
    public void createMappingDefault() {
        // create ordered target column names
        LinkedHashSet<String> columnNames = new LinkedHashSet<>();
        columnNames.add("bb");
        columnNames.add("aa");
        columnNames.add("cc");

        // create read input tuple
        LinkedHashMap<String, Object> namedValues = new LinkedHashMap<>();
        namedValues.put("aa", true);
        namedValues.put("bb", null);
        namedValues.put("cc", 42);
        Tuple input = new DummyTuple(namedValues, "sendername");

        // create field to column data source
        FieldMapper mapping = new FieldMapper(columnNames, null);
        FieldSource columnSource[] = mapping.createMapping(input);

        // expect values from fields "bb", "aa", "cc"
        assertEquals(null, columnSource[0].getValue(input));
        assertEquals(true, columnSource[1].getValue(input));
        assertEquals(42, columnSource[2].getValue(input));
    }

    @Test
    public void createMappingCustom() {
        // create ordered target column names
        LinkedHashSet<String> columnNames = new LinkedHashSet<>();
        columnNames.add("bb");
        columnNames.add("aa");
        columnNames.add("cc");

        // create read input tuple
        LinkedHashMap<String, Object> namedValues = new LinkedHashMap<>();
        namedValues.put("other", true);
        namedValues.put("bb", "not me");
        namedValues.put("cc", 42);
        Tuple input = new DummyTuple(namedValues, "sendername");

        // create custom mapping
        HashMap<String, String> columnToField = new HashMap<>();
        columnToField.put("aa", "cc");
        columnToField.put("cc", "other");

        // create field to column data source
        FieldMapper mapping = new FieldMapper(columnNames, columnToField);
        FieldSource columnSource[] = mapping.createMapping(input);

        // expect values from fields N/A, "cc", "other"
        assertEquals(null, columnSource[0].getValue(input));
        assertEquals(42, columnSource[1].getValue(input));
        assertEquals(true, columnSource[2].getValue(input));
    }

    @Test
    public void getCachedMapping() {
        // create ordered target column names
        LinkedHashSet<String> columnNames = new LinkedHashSet<>();
        columnNames.add("bb");
        columnNames.add("aa");
        columnNames.add("cc");

        // create read input tuples
        LinkedHashMap<String, Object> namedValues = new LinkedHashMap<>();
        namedValues.put("aa", true);
        namedValues.put("bb", null);
        namedValues.put("cc", 42);

        Tuple input_1 = new DummyTuple(namedValues, "sender_1");
        Tuple input_2 = new DummyTuple(namedValues, "sender_2");
        Tuple input_3 = new DummyTuple(namedValues, "sender_1");

        // create field to column data source
        FieldMapper mapping = new FieldMapper(columnNames, null);
        FieldSource columnSource1[] = mapping.getCachedMapping(input_1);
        FieldSource columnSource2[] = mapping.getCachedMapping(input_2);
        FieldSource columnSource3[] = mapping.getCachedMapping(input_3);

        // check returned instances
        assertTrue(columnSource1 != columnSource2);
        assertTrue(columnSource1 == columnSource3);
    }
}
