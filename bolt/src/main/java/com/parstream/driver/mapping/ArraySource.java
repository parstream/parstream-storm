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

/**
 * Value accessor of fields from native object array instances.
 */
public class ArraySource implements DataSource<Object[]> {

    private int _index;

    /**
     * Construct empty accessor returning null values.
     */
    public ArraySource() {
        _index = -1;
    }

    /**
     * Construct accessor returning array element by index.
     *
     * @param index
     *            index of array element
     */
    public ArraySource(int index) {
        _index = index;
    }

    @Override
    public Object getValue(Object[] input) {
        return _index == -1 ? null : input[_index];
    }
}
