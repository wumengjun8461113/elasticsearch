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
package org.elasticsearch.test.io;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public final class MockFileChannel extends FileChannel {

    FileChannel delegate;

    public MockFileChannel(FileChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        // delay buffer read..
        int willActuallyRead = ESTestCase.randomInt(dst.remaining());
        ByteBuffer mockDst = dst.duplicate();
        mockDst.limit(mockDst.position() + willActuallyRead);
        try {
            return delegate.read(mockDst);
        } finally {
            dst.position(mockDst.position());
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return delegate.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        // delay buffer write..
        int willActuallyWrite = ESTestCase.randomInt(src.remaining());
        ByteBuffer mockSrc = src.duplicate();
        mockSrc.limit(mockSrc.position() + willActuallyWrite);
        try {
            return delegate.write(mockSrc);
        } finally {
            src.position(mockSrc.position());
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        return delegate.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        return delegate.truncate(size);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        delegate.close();
    }
}
