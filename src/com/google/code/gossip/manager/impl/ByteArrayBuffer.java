/*
 * Copyright 2005-2006 Sun Microsystems, Inc. All Rights Reserved. DO NOT ALTER
 * OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER. This code is free software;
 * you can redistribute it and/or modify it under the terms of the GNU General
 * Public License version 2 only, as published by the Free Software Foundation.
 * Sun designates this particular file as subject to the "Classpath" exception
 * as provided by Sun in the LICENSE file that accompanied this code. This code
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU General Public License version 2 for more
 * details (a copy is included in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License version 2
 * along with this work; if not, write to the Free Software Foundation, Inc., 51
 * Franklin St, Fifth Floor, Boston, MA 02110-1301 USA. Please contact Sun
 * Microsystems, Inc., 4150 Network Circle, Santa Clara, CA 95054 USA or visit
 * www.sun.com if you need additional information or have any questions.
 */

package com.google.code.gossip.manager.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Read/write buffer that stores a sequence of bytes.
 * 
 * <p>
 * It works in a way similar to {@link ByteArrayOutputStream} but this class
 * works better in the following ways:
 * 
 * <ol>
 * <li>no synchronization
 * <li>offers a {@link #newInputStream()} that creates a new {@link InputStream}
 * that won't cause buffer reallocation.
 * <li>less parameter correctness checking
 * <li>offers a {@link #write(InputStream)} method that reads the entirety of
 * the given {@link InputStream} without using a temporary buffer.
 * </ol>
 * 
 * @author Kohsuke Kawaguchi
 */
public class ByteArrayBuffer extends OutputStream {
    /**
     * The buffer where data is stored.
     */
    protected byte[] buf;

    /**
     * The number of valid bytes in the buffer.
     */
    private int count;

    /**
     * Creates a new byte array output stream. The buffer capacity is initially
     * 32 bytes, though its size increases if necessary.
     */
    public ByteArrayBuffer() {
        this(32);
    }

    public ByteArrayBuffer(final byte[] data) {
        buf = data;
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the
     * specified size, in bytes.
     * 
     * @param size
     *            the initial size.
     * @throws IllegalArgumentException
     *             if size is negative.
     */
    public ByteArrayBuffer(final int size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }
        buf = new byte[size];
    }

    @Override
    public void close() throws IOException {
    }

    private void ensureCapacity(final int newcount) {
        if (newcount > buf.length) {
            final byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
    }

    /**
     * Gets the underlying buffer that this {@link ByteArrayBuffer} uses. It's
     * never small than its {@link #size()}.
     * 
     * Use with caution.
     */
    public final byte[] getRawData() {
        return buf;
    }

    /**
     * Creates a new {@link InputStream} that reads from this buffer.
     */
    public final InputStream newInputStream() {
        return new ByteArrayInputStream(buf, 0, count);
    }

    /**
     * Creates a new {@link InputStream} that reads a part of this bfufer.
     */
    public final InputStream newInputStream(final int start, final int length) {
        return new ByteArrayInputStream(buf, start, length);
    }

    public final void reset() {
        count = 0;
    }

    public final int size() {
        return count;
    }

    /**
     * Gets the <b>copy</b> of exact-size byte[] that represents the written
     * data.
     * 
     * <p>
     * Since this method needs to allocate a new byte[], this method will be
     * costly.
     * 
     * @deprecated this method causes a buffer reallocation. Use it only when
     *             you have to.
     */
    @Deprecated
    public final byte[] toByteArray() {
        final byte newbuf[] = new byte[count];
        System.arraycopy(buf, 0, newbuf, 0, count);
        return newbuf;
    }

    /**
     * Decodes the contents of this buffer by the default encoding and returns
     * it as a string.
     * 
     * <p>
     * Meant to aid debugging, but no more.
     */
    @Override
    public String toString() {
        return new String(buf, 0, count);
    }

    @Override
    public final void write(final byte b[], final int off, final int len) {
        final int newcount = count + len;
        ensureCapacity(newcount);
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
    }

    /**
     * Reads all the data of the given {@link InputStream} and appends them into
     * this buffer.
     * 
     * @throws IOException
     *             if the read operation fails with an {@link IOException}.
     */
    public final void write(final InputStream in) throws IOException {
        while (true) {
            final int cap = buf.length - count; // the remaining buffer space
            final int sz = in.read(buf, count, cap);
            if (sz < 0) {
                return; // hit EOS
            }
            count += sz;

            if (cap == sz) {
                ensureCapacity(buf.length * 2); // buffer filled up.
            }
        }
    }

    @Override
    public final void write(final int b) {
        final int newcount = count + 1;
        ensureCapacity(newcount);
        buf[count] = (byte) b;
        count = newcount;
    }

    public final void writeTo(final OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }
}
