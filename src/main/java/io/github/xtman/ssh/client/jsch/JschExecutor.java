package io.github.xtman.ssh.client.jsch;

import java.io.IOException;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.Executor;

public class JschExecutor implements io.github.xtman.ssh.client.Executor {

    private JschConnection _connection;
    private com.jcraft.jsch.ChannelExec _channel;
    private String _remoteBaseDir;
    private String _encoding;
    protected boolean verbose;

    JschExecutor(JschConnection connection, com.jcraft.jsch.ChannelExec channel, String remoteBaseDir, String encoding,
            boolean verbose) {
        _connection = connection;
        _channel = channel;
        _remoteBaseDir = (remoteBaseDir == null || remoteBaseDir.trim().isEmpty())
                ? Connection.DEFAULT_REMOTE_BASE_DIRECTORY : remoteBaseDir.trim();
        _encoding = encoding;
        this.verbose = verbose;
    }

    @Override
    public void close() throws IOException {
        _channel.disconnect();
    }

    @Override
    public Connection connection() {
        return _connection;
    }

    @Override
    public void execute(String command) throws Throwable {
        // TODO Auto-generated method stub

    }

    @Override
    public final String channelType() {
        return Executor.CHANNEL_TYPE_NAME;
    }

    public String remoteBaseDirectory() {
        return _remoteBaseDir;
    }

    public void setRemoteBaseDirectory(String baseDir) {
        if (baseDir == null || baseDir.trim().isEmpty()) {
            _remoteBaseDir = Connection.DEFAULT_REMOTE_BASE_DIRECTORY;
        } else {
            _remoteBaseDir = baseDir.trim();
        }
    }

    @Override
    public String encoding() {
        return _encoding;
    }

    @Override
    public void setEncoding(String encoding) {
        _encoding = encoding;
    }

    @Override
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public boolean verbose() {
        return this.verbose;
    }

}
