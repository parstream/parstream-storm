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

import java.util.LinkedHashMap;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

/**
 * Tuple implementation for internal processing unit test.
 */
public class DummyTuple implements Tuple {

    String[] _names;
    Object[] _values;

    String _sourceComponent;

    public DummyTuple(LinkedHashMap<String, Object> namedValues, String sourceComponent) {
        _names = namedValues.keySet().toArray(new String[0]);
        _values = namedValues.values().toArray();
        _sourceComponent = sourceComponent;
    }

    @Override
    public int size() {
        return _names.length;
    }

    @Override
    public boolean contains(String field) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Fields getFields() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int fieldIndex(String field) {
        for (int i = 0; i < _names.length; i++) {
            if (_names[i] == field) {
                return i;
            }
        }
        throw new IllegalArgumentException("field name '" + field + "' not found");
    }

    @Override
    public List<Object> select(Fields selector) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getValue(int i) {
        return _values[i];
    }

    @Override
    public String getString(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getInteger(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getLong(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Short getShort(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Byte getByte(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Double getDouble(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Float getFloat(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getValueByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStringByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getIntegerByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getLongByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean getBooleanByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Short getShortByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Byte getByteByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Double getDoubleByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Float getFloatByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getBinaryByField(String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Object> getValues() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSourceComponent() {
        return _sourceComponent;
    }

    @Override
    public int getSourceTask() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageId getMessageId() {
        // TODO Auto-generated method stub
        return null;
    }
}
