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
package com.parstream.adaptor.storm.mapping;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.parstream.driver.mapping.RowCreator;

import backtype.storm.tuple.Tuple;

/**
 * Handle mapping between {@link Tuple} field names and ParStream column names.
 */
public class FieldMapper {
    private Set<String> _columnNames;
    private Map<String, FieldSource[]> _mapCache = new HashMap<>();

    /**
     * Prepare mapping regarding ParStream column names and an optional custom
     * name mapping.
     *
     * @param columnNames
     *            ordered collection of column names
     * @param columnToField
     *            optional custom mapping between column names and field names
     */
    public FieldMapper(Set<String> columnNames, Map<String, String> columnToField) {
        assert columnNames != null;
        assert!columnNames.isEmpty();

        if (columnToField == null) {
            // use default mapping (one-to-one)
            _columnNames = columnNames;
        } else {
            // use custom mapping, not mapped field names are null
            _columnNames = new LinkedHashSet<>();
            for (String name : columnNames) {
                _columnNames.add(columnToField.get(name));
            }
        }
    }

    /**
     * Create new value accessor array for {@link RowCreator} usage.
     *
     * @param input
     *            storm tuple input
     * @return value accessor array
     */
    public FieldSource[] createMapping(Tuple input) {
        FieldSource[] mapping = new FieldSource[_columnNames.size()];

        int i = 0;
        for (String name : _columnNames) {
            if (name == null) {
                mapping[i] = new FieldSource();
            } else {
                mapping[i] = new FieldSource(input.fieldIndex(name));
            }
            i++;
        }
        return mapping;
    }

    /**
     * Retrieve value accessor array for {@link RowCreator} usage.
     *
     * This implementation will cache mappings regarding source components.
     *
     * @param input
     *            storm tuple input
     * @return value accessor array
     */
    public FieldSource[] getCachedMapping(Tuple input) {
        String senderType = input.getSourceComponent();
        FieldSource[] mapping = _mapCache.get(senderType);
        if (mapping == null) {
            mapping = createMapping(input);
            _mapCache.put(senderType, mapping);
        }
        return mapping;
    }
}
