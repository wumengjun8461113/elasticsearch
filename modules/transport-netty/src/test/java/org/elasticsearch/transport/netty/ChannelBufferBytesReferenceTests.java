/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport.netty;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.io.MockFileChannel;
import org.elasticsearch.transport.ChannelBufferBytesReference;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class ChannelBufferBytesReferenceTests extends ESTestCase{

    public void testWriteFromChannel() throws IOException {
        try (FileChannel randomAccessFile = FileChannel.open(createTempFile(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            FileChannel fileChannel = new MockFileChannel(randomAccessFile);
            byte[] randomBytes = randomUnicodeOfLength(scaledRandomIntBetween(10, 100000)).getBytes("UTF-8");

            int length = randomIntBetween(1, randomBytes.length / 2);
            int offset = randomIntBetween(0, randomBytes.length - length);
            ByteBuffer byteBuffer = ByteBuffer.wrap(randomBytes);
            ChannelBuffer source = new ByteBufferBackedChannelBuffer(byteBuffer);
            ChannelBufferBytesReference.writeToChannel(source, offset, length, fileChannel);

            BytesReference copyRef = new BytesArray(Channels.readFromFileChannel(fileChannel, 0, length));
            BytesReference sourceRef = new BytesArray(randomBytes, offset, length);

            assertTrue("read bytes didn't match written bytes", sourceRef.equals(copyRef));
        }
    }
}
