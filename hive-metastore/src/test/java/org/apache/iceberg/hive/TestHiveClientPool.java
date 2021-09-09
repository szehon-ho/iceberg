/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.AssertHelpers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHiveClientPool {

  private static final String HIVE_SITE_CONTENT = "<?xml version=\"1.0\"?>\n" +
          "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
          "<configuration>\n" +
          "  <property>\n" +
          "    <name>hive.metastore.sasl.enabled</name>\n" +
          "    <value>true</value>\n" +
          "  </property>\n" +
          "</configuration>\n";

  HiveClientPool clients;

  @Before
  public void before() {
    HiveClientPool clientPool = new HiveClientPool(2, new Configuration());
    clients = Mockito.spy(clientPool);
  }

  @After
  public void after() {
    clients.close();
    clients = null;
  }

  @Test
  public void testConf() {
    HiveConf conf = createHiveConf();
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:/mywarehouse/");

    HiveClientPool clientPool = new HiveClientPool(10, conf);
    HiveConf clientConf = clientPool.hiveConf();

    Assert.assertEquals(conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    Assert.assertEquals(10, clientPool.poolSize());

    // 'hive.metastore.sasl.enabled' should be 'true' as defined in xml
    Assert.assertEquals(conf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname),
            clientConf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));
    Assert.assertTrue(clientConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL));
  }

  private HiveConf createHiveConf() {
    HiveConf hiveConf = new HiveConf();
    try (InputStream inputStream = new ByteArrayInputStream(HIVE_SITE_CONTENT.getBytes(StandardCharsets.UTF_8))) {
      hiveConf.addResource(inputStream, "for_test");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return hiveConf;
  }

  @Test
  public void testNewClientFailure() {
    Mockito.doThrow(new RuntimeException("Connection exception")).when(clients).newClient();
    AssertHelpers.assertThrows("Should throw exception", RuntimeException.class,
            "Connection exception", () -> clients.run(Object::toString));
  }

  @Test
  public void testGetTablesFailsForNonReconnectableException() throws Exception {
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();
    Mockito.doThrow(new MetaException("Another meta exception"))
            .when(hmsClient).getTables(Mockito.anyString(), Mockito.anyString());
    AssertHelpers.assertThrows("Should throw exception", MetaException.class,
            "Another meta exception", () -> clients.run(client -> client.getTables("default", "t")));
  }
}
