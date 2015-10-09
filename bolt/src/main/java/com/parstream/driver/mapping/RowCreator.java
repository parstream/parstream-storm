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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ColumnInfo.Type;
import com.parstream.driver.datatypes.AutoCastException;
import com.parstream.driver.datatypes.OutOfRangeException;
import com.parstream.driver.datatypes.PsBitVector8;
import com.parstream.driver.datatypes.PsClob;
import com.parstream.driver.datatypes.PsDate;
import com.parstream.driver.datatypes.PsDouble;
import com.parstream.driver.datatypes.PsFloat;
import com.parstream.driver.datatypes.PsInt16;
import com.parstream.driver.datatypes.PsInt32;
import com.parstream.driver.datatypes.PsInt64;
import com.parstream.driver.datatypes.PsInt8;
import com.parstream.driver.datatypes.PsShortDate;
import com.parstream.driver.datatypes.PsTime;
import com.parstream.driver.datatypes.PsTimestamp;
import com.parstream.driver.datatypes.PsUint16;
import com.parstream.driver.datatypes.PsUint32;
import com.parstream.driver.datatypes.PsUint64;
import com.parstream.driver.datatypes.PsUint8;
import com.parstream.driver.datatypes.PsValue;
import com.parstream.driver.datatypes.PsVarstring;

/**
 * Create new ParStream rows for inserting by the streaming import API from
 * input data with a ordered list of value accessors.
 *
 * @param <InputType>
 *            type of input data for a single row
 */
public class RowCreator<InputType> {

    private static Map<Type, PsValue> _columntypes;
    private Map<String, PsValue> _columns;

    static {
        _columntypes = new HashMap<Type, PsValue>();
        _columntypes.put(Type.BITVECTOR8, new PsBitVector8());
        _columntypes.put(Type.BLOB, new PsClob());
        _columntypes.put(Type.DATE, new PsDate());
        _columntypes.put(Type.DOUBLE, new PsDouble());
        _columntypes.put(Type.FLOAT, new PsFloat());
        _columntypes.put(Type.INT16, new PsInt16());
        _columntypes.put(Type.INT32, new PsInt32());
        _columntypes.put(Type.INT64, new PsInt64());
        _columntypes.put(Type.INT8, new PsInt8());
        _columntypes.put(Type.SHORTDATE, new PsShortDate());
        _columntypes.put(Type.TIME, new PsTime());
        _columntypes.put(Type.TIMESTAMP, new PsTimestamp());
        _columntypes.put(Type.UINT16, new PsUint16());
        _columntypes.put(Type.UINT32, new PsUint32());
        _columntypes.put(Type.UINT64, new PsUint64());
        _columntypes.put(Type.UINT8, new PsUint8());
        _columntypes.put(Type.VARSTRING, new PsVarstring());
    }

    /**
     * Construct ParStream row creator and initialize row template from
     * ColumnInfo array.
     *
     * @param columnInfo
     *            column information array provided by ParStream's streaming
     *            import API
     *
     * @throws IllegalStateException
     *             if an unknown column type was reported by the database
     */
    public RowCreator(ColumnInfo[] columnInfo) {
        _columns = new LinkedHashMap<>();
        for (int i = 0; i < columnInfo.length; i++) {
            try {
                _columns.put(columnInfo[i].getName(),
                        _columntypes.get(columnInfo[i].getType()).getClass().newInstance());
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new IllegalStateException("cannot create column value type " + columnInfo[i].getType().name(),
                        ex);
            }
        }
    }

    /**
     * Construct ParStream row creator.
     *
     * @param rowTemplate
     *            row template for generating new rows
     */
    public RowCreator(LinkedHashMap<String, PsValue> rowTemplate) {
        assert rowTemplate != null;
        _columns = rowTemplate;
    }

    /**
     * @return number of columns used by this row creator
     */
    public int getRowSize() {
        return _columns.size();
    }

    /**
     * @return ordered set of column names
     */
    public Set<String> getColumnNames() {
        return _columns.keySet();
    }

    /**
     * Convert input object into a ParStream's Object array for inserting into
     * ParStream database by the ParStream streaming API.
     *
     * @param input
     *            Object for extracting and converting data into ParStream
     *            column data types
     * @param source
     *            Mapping implementation for input's object type
     * 
     * @return Object array for inserting by streaming import API
     *
     * @throws AutoCastException
     *             if one input value type cannot be automatically cast into
     *             this ParStream column data type
     * @throws OutOfRangeException
     *             if one input value is out of range for this ParStream column
     *             data type
     */
    public Object[] createRow(InputType input, DataSource<InputType>[] source)
            throws AutoCastException, OutOfRangeException {
        assert source.length == _columns.size();
        Object[] row = new Object[_columns.size()];
        int i = 0;
        for (PsValue column : _columns.values()) {
            row[i] = column.convert(source[i].getValue(input));
            i++;
        }
        return row;
    }
}
