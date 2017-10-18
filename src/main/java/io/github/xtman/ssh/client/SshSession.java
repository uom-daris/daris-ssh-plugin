package io.github.xtman.ssh.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ch.ethz.ssh2.Session;

public class SshSession implements Closeable {

    public static final String DEFAULT_ENCODING = "UTF-8";

    private SshConnection _connection;
    private Session _session;

    SshSession(SshConnection connection, Session session) {
        _connection = connection;
        _session = session;
    }

    public void execCommand(String command, String charsetName) throws IOException {
        _session.execCommand(command, charsetName);
    }

    public void execCommand(String command) throws IOException {
        _session.execCommand(command);
    }

    public int exitStatus() {
        return _session.getExitStatus();
    }

    public InputStream inputStream() {
        return _session.getStdout();
    }

    public OutputStream outputStream() {
        return _session.getStdin();
    }

    public void close() throws IOException {
        if (_session != null) {
            try {
                _session.close();
            } finally {
                _connection.removeSession(_session);
            }
        }
    }

}
