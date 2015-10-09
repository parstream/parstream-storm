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
package com.parstream.adaptor.storm.config;

import java.io.Serializable;
import java.util.Map;

/**
 * ParStream's storm bolt configuration POJ.
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = 1L;

    /*
     * ParStream database connection
     */
    public String host;
    public String port;
    public String username;
    public String password;

    /*
     * Column Mapping
     */
    public String tableName;
    public boolean failOnMissingField = true;
    public Map<String, String> columnToField;

    /*
     * Auto Commit
     */
    public Long autoCommitCount;
    public Long autoCommitTimespan;
    public CommitOrder commitOrder = CommitOrder.INPUT_FIRST;

    @Override
    public String toString() {
        return "Configuration(host=" + host + ",port=" + port + ",tableName=" + tableName + ")";
    }
}
