/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.flume;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.apache.phoenix.util.DateUtil;

public enum DefaultKeyGenerator implements KeyGenerator {

    UUID  {

        @Override
        public String generate() {
           return String.valueOf(java.util.UUID.randomUUID());
        }

        @Override
        public int length() {
            return 36;
        }

    },
    BASE64  {

        @Override
        public String generate() {
            java.util.UUID uuid = java.util.UUID.randomUUID();
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return B64.encodeBase64URLSafeString(bb.array());
        }
        @Override
        public int length() {
            return 22;
        }

    },
    TIMESTAMP {

        @Override
        public String generate() {
            java.sql.Timestamp ts = new Timestamp(System.currentTimeMillis());
            return DateUtil.DEFAULT_DATE_FORMATTER.format(ts);
        }

        @Override
        public int length() {
            return 19;
        }
    },
    DATE {
        
        @Override
        public String generate() {
            Date dt =  new Date(System.currentTimeMillis());
            return DateUtil.DEFAULT_DATE_FORMATTER.format(dt);
        }

        @Override
        public int length() {
            return 19;
        }
    },
    RANDOM {

        @Override
        public String generate() {
            return String.valueOf(new Random().nextLong());
        }

        @Override
        public int length() {
            return 20;
        }
    },
    NANOTIMESTAMP {

        @Override
        public String generate() {
            return String.valueOf(System.nanoTime());
        }

        @Override
        public int length() {
            return 20;
        }
    };

    private static Base64 B64 = new Base64();
}
