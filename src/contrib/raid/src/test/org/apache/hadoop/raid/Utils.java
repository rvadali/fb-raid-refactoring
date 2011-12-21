/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A place for some static methods used by the Raid unit test
 */
public class Utils {

  public static final Log LOG = LogFactory.getLog(Utils.class);

  /**
   * Load typical codecs for unit test use
   */
  public static void loadTestCodecs() {
    Utils.loadTestCodecs(5, 1, 3, "/raid", "/raidrs");
  }

  /**
   * Load RS and XOR codecs with given parameters
   */
  public static void loadTestCodecs(int stripeLength, int xorParityLength, int rsParityLength,
      String xorParityDirectory, String rsParityDirectory) {
    Codec.clearCodecs();
    {
      Codec codec = new Codec("xor",
                              xorParityLength,
                              stripeLength,
                              "org.apache.hadoop.raid.XorCode",
                              xorParityDirectory,
                              100,
                              "",
                              "/tmp/" + xorParityDirectory,
                              "/tmp" + xorParityDirectory + "_har");
      Codec.addCodec(codec);
    }
    {
      Codec codec = new Codec("rs",
                              rsParityLength,
                              stripeLength,
                              "org.apache.hadoop.raid.ReedSolomonCode",
                              rsParityDirectory,
                              200,
                              "",
                              "/tmp" + rsParityDirectory,
                              "/tmp" + rsParityDirectory + "_har");
      Codec.addCodec(codec);
    }
    LOG.info("Test codec loaded");
    for (Codec c : Codec.getCodecs()) {
      LOG.info("Loaded raid code:" + c.id);
    }
  }

}
