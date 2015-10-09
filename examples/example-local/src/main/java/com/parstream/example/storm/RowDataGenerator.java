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
package com.parstream.example.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RowDataGenerator extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private final Logger _log = LoggerFactory.getLogger(RowDataGenerator.class);

    private SpoutOutputCollector _out;

    private Long _maxcount;
    private long _msgId;

    public RowDataGenerator(long maxcount) {
        _maxcount = maxcount;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _log.info("open spout");
        _out = collector;
        _msgId = 0;
    }

    @Override
    public void nextTuple() {
        if (_msgId < _maxcount) {
            _msgId += 1;
            _log.info("send value " + _msgId);

            String name = "text for id " + _msgId;
            double count = Math.floor(Math.random() * 256);

            // emit message with my own tracking message_id
            _out.emit(new Values(_msgId, name, (long) count), _msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "name", "number"));
    }

    @Override
    public void ack(Object msgId) {
        _log.info("ack msgId=" + msgId);

        // if we have all acknowledges, we terminate ourself
        if (_maxcount.equals(msgId)) {
            throw new IllegalStateException("successfully process last message; terminating topology");
        }
    }

    @Override
    public void fail(Object msgId) {
        _log.info("fail msgId=" + msgId);
    }
}
