/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fatri.pool.hbase;

public interface HbaseConfig {

    /**
     * DEFAULT_HOST
     */
    String DEFAULT_HOST = "localhost";
    /**
     * DEFAULT_PORT
     */
    String DEFAULT_PORT = "2181";
    /**
     * DEFAULT_MASTER
     */
    String DEFAULT_MASTER = null;
    /**
     * DEFAULT_ROOTDIR
     */
    String DEFAULT_ROOTDIR = null;

    /**
     * ZOOKEEPER_QUORUM_PROPERTY
     */
    String ZOOKEEPER_QUORUM_PROPERTY = "hbase.zookeeper.quorum";
    /**
     * ZOOKEEPER_CLIENTPORT_PROPERTY
     */
    String ZOOKEEPER_CLIENTPORT_PROPERTY = "hbase.zookeeper.property.clientPort";
    /**
     * MASTER_PROPERTY
     */
    String MASTER_PROPERTY = "hbase.master";
    /**
     * ROOTDIR_PROPERTY
     */
    String ROOTDIR_PROPERTY = "hbase.rootdir";

}
