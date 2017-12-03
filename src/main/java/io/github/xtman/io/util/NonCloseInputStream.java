package io.github.xtman.io.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class NonCloseInputStream extends FilterInputStream {

    public NonCloseInputStream(InputStream in) {
        super(in);
    }

    @Override
    public void close() throws IOException {
        // DO NOT close
    }

}
