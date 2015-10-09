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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.parstream.adaptor.storm.StreamingImportBolt;
import com.parstream.adaptor.storm.config.CommitOrder;
import com.parstream.adaptor.storm.config.Configuration;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class LocalTopology {

    public static void main(String[] args) throws Exception {
        final Logger log = LoggerFactory.getLogger(LocalTopology.class);
        TopologyBuilder builder = new TopologyBuilder();

        /*
         * configure ParStream's streaming import bolt
         */
        Configuration siConfig = new Configuration();
        siConfig.commitOrder = CommitOrder.PARSTREAM_FIRST;

        // read my properties from supervisor installation
        log.info("STORM_CONF_DIR = " + System.getenv().get("STORM_CONF_DIR"));
        Properties database = new Properties();
        File psConigFile = new File(System.getenv().get("STORM_CONF_DIR"), "database.properties");
        try (InputStream is = new FileInputStream(psConigFile)) {
            log.info("load external properties " + psConigFile.getName());
            database.load(is);

            siConfig.host = database.getProperty("parstream.server.address");
            siConfig.port = database.getProperty("parstream.server.port");
            siConfig.tableName = database.getProperty("parstream.import.table");
            String commitCount = database.getProperty("parstream.autocommit.rowcount");
            if (commitCount != null) {
                siConfig.autoCommitCount = Long.valueOf(commitCount);
            }

            String timespan = database.getProperty("parstream.autocommit.timespan");
            if (timespan != null) {
                siConfig.autoCommitTimespan = Long.valueOf(timespan);
            }
        } catch (IOException ex) {
            // we did not find any configuration file, use example values
            log.info("use default properties");
            siConfig.host = "127.0.0.1";
            siConfig.port = "8989";
            siConfig.tableName = "MyTable";
            siConfig.autoCommitCount = 1l;
        }

        /*
         * create storm topology
         * 
         * We're producing 13 rows and committing them into the ParStream
         * database.
         */
        builder.setSpout("source", new RowDataGenerator(13));
        builder.setBolt("dbsink", new StreamingImportBolt(siConfig)).shuffleGrouping("source");

        /*
         * Configure and run topology.
         */
        Config conf = new Config();
        // conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(42000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
