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

import com.parstream.driver.mapping.DataSource;

import backtype.storm.tuple.Tuple;

/**
 * Value accessor of fields from Apache Storm {@link Tuple} instances.
 */
public class FieldSource implements DataSource<Tuple> {

    private int _index;

    /**
     * Construct empty accessor returning null values.
     */
    public FieldSource() {
        _index = -1;
    }

    /**
     * Construct accessor returning tuple field by index.
     *
     * @param index
     *            index of field in tuple input
     */
    public FieldSource(int index) {
        _index = index;
    }

    @Override
    public Object getValue(Tuple input) {
        return _index == -1 ? null : input.getValue(_index);
    }
}
