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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.parstream.adaptor.storm.config.CommitOrder;
import com.parstream.adaptor.storm.config.Configuration;
import com.parstream.adaptor.storm.mapping.FieldMapper;
import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamConnection;
import com.parstream.driver.ParstreamDriver;
import com.parstream.driver.ParstreamException;
import com.parstream.driver.ParstreamFatalException;
import com.parstream.driver.datatypes.AutoCastException;
import com.parstream.driver.datatypes.OutOfRangeException;
import com.parstream.driver.mapping.RowCreator;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StreamingImportBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger _log = LoggerFactory.getLogger(StreamingImportBolt.class);

    private Configuration _config;
    transient private OutputCollector _out;

    transient private ParstreamDriver _database;
    transient private FieldMapper _mapping;
    transient private RowCreator<Tuple> _rowCreator;

    transient private long _rowCount;
    transient private Timer _timer;
    transient private TimerTask _autoCommitTask;
    transient private List<Tuple> _acknowledgeList;

    /**
     * Construct ParStream's sink bolt.
     * 
     * @param config
     *            configuration for this bolt
     */
    public StreamingImportBolt(Configuration config) {
        _log.info("creating with " + config.toString());
        _config = config;

        if (_config.autoCommitCount == null && _config.autoCommitTimespan == null) {
            throw new IllegalArgumentException("autoCommitCount and autoCommitTimespan are null");
        }
    }

    @Override
    public void execute(Tuple input) {
        synchronized (_out) {
            if (input != null) {
                insertRow(input);
            }
            // reached auto commit row count?
            if (_config.autoCommitCount != null && _rowCount >= _config.autoCommitCount) {
                commitRows();
                prepareInsert();
                if (_config.autoCommitTimespan != null) {
                    // restart auto commit timer from now
                    _autoCommitTask.cancel();
                    startCommitTimer();
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _log.info("prepare bolt");
        _out = collector;

        try {
            _log.info("connect to database " + _config.host + ":" + _config.port);
            _database = new ParstreamConnection();
            _database.createHandle();
            _database.connect(_config.host, _config.port, _config.username, _config.password);
        } catch (ParstreamFatalException ex) {
            throw new IllegalStateException("connect to database failed", ex);
        }

        ColumnInfo[] columns;
        try {
            columns = _database.listImportColumns(_config.tableName);
        } catch (ParstreamFatalException | ParstreamException ex) {
            throw new IllegalStateException("read table definition failed", ex);
        }

        _rowCreator = new RowCreator<Tuple>(columns);
        _mapping = new FieldMapper(_rowCreator.getColumnNames(), _config.columnToField);

        _rowCount = 0;
        _acknowledgeList = new LinkedList<>();
        prepareInsert();

        // set up commit timer if needed
        if (_config.autoCommitTimespan != null) {
            _timer = new Timer();
            startCommitTimer();
        }
    }

    private void startCommitTimer() {
        _autoCommitTask = new TimerTask() {
            @Override
            public void run() {
                _log.info("auto commit timeout reached");
                synchronized (_out) {
                    commitRows();
                    prepareInsert();
                }
            }
        };
        _timer.schedule(_autoCommitTask, _config.autoCommitTimespan * 1000L, _config.autoCommitTimespan * 1000L);
    }

    @Override
    public void cleanup() {
        _log.info("clean up bolt resources");
        if (_config.autoCommitTimespan != null) {
            _timer.cancel();
        }
        _database.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to declare here
    }

    private void prepareInsert() {
        try {
            _log.info("prepare insert into " + _config.tableName);
            _database.prepareInsert(_config.tableName);
        } catch (ParstreamException | ParstreamFatalException ex) {
            throw new IllegalStateException("prepare insert failed", ex);
        }
    }

    private void insertRow(Tuple input) {
        // convert input
        Object[] row;
        try {
            row = _rowCreator.createRow(input, _mapping.getCachedMapping(input));
            if (_log.isDebugEnabled()) {
                StringBuffer rowAsText = new StringBuffer("row values are: ");
                for (int i = 0; i < _rowCreator.getRowSize(); i++) {
                    rowAsText.append(row[i].toString()).append(",");
                }
                _log.debug(rowAsText.toString());
            }
        } catch (AutoCastException | OutOfRangeException ex) {
            _log.error("convert input failed: " + input);
            throw new IllegalStateException("converting input failed", ex);
        }

        // insert row into database
        try {
            _database.rawInsert(row);
            _acknowledgeList.add(input);
            _rowCount++;
        } catch (ParstreamException | ParstreamFatalException ex) {
            throw new IllegalStateException("insert row failed", ex);
        }
    }

    private void commitRows() {
        try {
            _log.info("committing " + _rowCount + " row(s)");
            _rowCount = 0;

            if (_config.commitOrder == CommitOrder.PARSTREAM_FIRST) {
                _database.commit();
            }
            for (Tuple input : _acknowledgeList) {
                _out.ack(input);
            }
            if (_config.commitOrder == CommitOrder.INPUT_FIRST) {
                _database.commit();
            }
        } catch (ParstreamFatalException ex) {
            throw new IllegalStateException("committing to database failed", ex);
        }
    }
}
