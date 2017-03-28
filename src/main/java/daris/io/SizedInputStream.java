package daris.io;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SizedInputStream extends FilterInputStream {

    private long _size = -1L;
    private long _read = 0;
    private long _mark = 0;
    private boolean _canClose;

    public SizedInputStream(InputStream in, long size, boolean canClose) {
        super(in);
        _size = size;
        _canClose = canClose;
    }

    public SizedInputStream(InputStream in, long size) {
        this(in, size, false);
    }

    @Override
    public synchronized int read() throws IOException {
        if (_read == _size) {
            return -1;
        }
        int b = in.read();
        if (b != -1) {
            _read++;
        }
        return b;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len)
            throws IOException {
        if (_read == _size) {
            return -1;
        }
        if (_size > 0 && _read + len > _size) {
            len = (int) (_size - _read);
        }
        int n = in.read(b, off, len);
        if (n == -1) {
            if (_size < 0) {
                return -1;
            }
            throw new EOFException(
                    "End of stream found (EOF). Attempting to read " + len
                            + " byte(s). Read " + _read
                            + " byte(s) so far. Expected to read a total of "
                            + _size + " byte(s)");
        } else {
            if (n < -1) {
                throw new IOException(
                        "Underlying stream indicated it has read less than -1 bytes: "
                                + n);
            }
            _read += n;
            return n;
        }
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (_read == _size) {
            return 0;
        }
        if (_size > 0 && _read + n > _size) {
            n = _size - _read;
        }
        long skipped = in.skip(n);
        _read += skipped;
        return skipped;
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
        _mark = _read;
    }

    @Override
    public synchronized void reset() throws IOException {
        /*
         * A call to reset can still succeed if mark is not supported, but the
         * resulting stream position is undefined, so it's not allowed here.
         */
        if (!markSupported()) {
            throw new IOException("Mark not supported.");
        }
        in.reset();
        _read = _mark;
    }

    @Override
    public synchronized int available() throws IOException {
        if (bytesRemaining() == 0) {
            return 0;
        }
        return in.available();
    }

    public synchronized long bytesRead() {
        return _read;
    }

    public synchronized long bytesRemaining() {
        if (_size < 0) {
            return -1;
        }
        if (_size == 0) {
            return 0;
        }
        return _size - _read;
    }

    @Override
    public void close() throws IOException {
        if (_canClose) {
            in.close();
        }
    }
}