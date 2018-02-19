package io.github.xtman.ssh.client.jsch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.UserInfo;

import io.github.xtman.ssh.client.ConnectionBuilder;
import io.github.xtman.ssh.client.ConnectionDetails;
import io.github.xtman.ssh.client.Executor;
import io.github.xtman.ssh.client.ScpClient;
import io.github.xtman.ssh.client.SftpClient;
import io.github.xtman.ssh.client.TransferClient;

public class JschConnection implements io.github.xtman.ssh.client.Connection {

    public static class Builder extends ConnectionBuilder {
        public Builder() {
            super(IMPL);
        }

        public Builder(String impl) {
            super(IMPL);
        }

        @Override
        public JschConnection build() throws Throwable {
            return (JschConnection) super.build();
        }

        @Override
        public Builder setImplementation(String impl) {
            return this;
        }
    }

    public static final String IMPL = "jsch";
    private Collection<com.jcraft.jsch.Channel> _channels;
    private ConnectionDetails _cxnDetails;
    private JSch _jsch;
    private com.jcraft.jsch.Session _jschSession;

    private int _maxChannels;

    private boolean _verbose;

    public JschConnection(ConnectionDetails cxnDetails, int maxChannels, boolean verbose) throws Throwable {
        _cxnDetails = cxnDetails;
        _maxChannels = maxChannels;
        _verbose = verbose;

        _channels = Collections.synchronizedCollection(new HashSet<com.jcraft.jsch.Channel>());

        _jsch = new JSch();
        _jsch.setKnownHosts(new ByteArrayInputStream(new byte[8192]));
        String hostKey = _cxnDetails.hostKey();
        if (hostKey != null) {
            _jsch.getHostKeyRepository()
                    .add(new HostKey(_cxnDetails.host(), Base64.getDecoder().decode(hostKey.getBytes())), userInfo());
        }
        /*
         * added user's identity (private & public keys)
         */
        if (_cxnDetails.privateKey() != null) {
            _jsch.addIdentity(_cxnDetails.username() + "_identity", _cxnDetails.privateKey().getBytes(),
                    _cxnDetails.publicKey() != null ? _cxnDetails.publicKey().getBytes() : null,
                    _cxnDetails.passphrase() != null ? _cxnDetails.passphrase().getBytes() : null);
        }

        /*
         * connect
         */
        _jschSession = _jsch.getSession(_cxnDetails.username(), _cxnDetails.host(), _cxnDetails.port());
        _jschSession.setConfig("StrictHostKeyChecking", _cxnDetails.hostKey() != null ? "yes" : "no");
        _jschSession.setUserInfo(userInfo());
        if (_verbose) {
            System.out.print("opening connection to " + _cxnDetails.host() + ":" + _cxnDetails.port() + " ...");
        }
        _jschSession.connect();
        if (_verbose) {
            System.out.println("done");
        }
    }

    private void clearChannels() {
        synchronized (_channels) {
            for (com.jcraft.jsch.Channel channel : _channels) {
                try {
                    if (!channel.isClosed()) {
                        channel.disconnect();
                    }
                } catch (Throwable e) {

                } finally {
                    _channels.remove(channel);
                    _channels.notifyAll();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            clearChannels();
        } finally {
            if (_verbose) {
                System.out.print("closing connection to " + _cxnDetails.host() + ":" + _cxnDetails.port() + " ...");
            }
            _jschSession.disconnect();
            if (_verbose) {
                System.out.println("done");
            }
        }
    }

    @Override
    public ConnectionDetails connectionDetails() {
        return _cxnDetails;
    }

    @Override
    public Executor createExecutor() throws Throwable {
        return this.createExecutor(DEFAULT_REMOTE_BASE_DIRECTORY, DEFAULT_ENCODING);
    }

    @Override
    public Executor createExecutor(String remoteBaseDir) throws Throwable {
        return this.createExecutor(remoteBaseDir, DEFAULT_ENCODING);
    }

    @Override
    public Executor createExecutor(String remoteBaseDir, String encoding) throws Throwable {
        return new JschExecutor(this, (com.jcraft.jsch.ChannelExec) openChannel(Executor.CHANNEL_TYPE_NAME),
                remoteBaseDir, encoding, verbose());
    }

    @Override
    public ScpClient createScpClient() throws Throwable {
        return this.createScpClient(DEFAULT_REMOTE_BASE_DIRECTORY, DEFAULT_ENCODING,
                TransferClient.DEFAULT_DIRECTORY_MODE, TransferClient.DEFAULT_FILE_MODE, false, false);
    }

    @Override
    public ScpClient createScpClient(String remoteBaseDir) throws Throwable {
        return this.createScpClient(remoteBaseDir, DEFAULT_ENCODING, TransferClient.DEFAULT_DIRECTORY_MODE,
                TransferClient.DEFAULT_FILE_MODE, false, false);
    }

    @Override
    public JschScpClient createScpClient(String remoteBaseDir, String encoding, Integer dirMode, Integer fileMode,
            boolean compress, boolean preserve) throws Throwable {
        return new JschScpClient(this, (com.jcraft.jsch.ChannelExec) openChannel(ScpClient.CHANNEL_TYPE_NAME),
                remoteBaseDir, encoding, dirMode, fileMode, compress, preserve, verbose());
    }

    @Override
    public SftpClient createSftpClient() throws Throwable {
        return this.createSftpClient(DEFAULT_REMOTE_BASE_DIRECTORY, DEFAULT_ENCODING,
                TransferClient.DEFAULT_DIRECTORY_MODE, TransferClient.DEFAULT_FILE_MODE, false, false);
    }

    @Override
    public SftpClient createSftpClient(String remoteBaseDir) throws Throwable {
        return this.createSftpClient(remoteBaseDir, DEFAULT_ENCODING, TransferClient.DEFAULT_DIRECTORY_MODE,
                TransferClient.DEFAULT_FILE_MODE, false, false);
    }

    @Override
    public JschSftpClient createSftpClient(String remoteBaseDir, String encoding, Integer dirMode, Integer fileMode,
            boolean compress, boolean preserve) throws Throwable {
        return new JschSftpClient(this, (com.jcraft.jsch.ChannelSftp) openChannel(SftpClient.CHANNEL_TYPE_NAME),
                remoteBaseDir, encoding, dirMode, fileMode, compress, preserve, verbose());
    }

    @Override
    public int maxChannels() {
        return _maxChannels;
    }

    private com.jcraft.jsch.Channel openChannel(String type) throws Throwable {
        synchronized (_channels) {
            if (_maxChannels > 0) {
                while (_channels.size() >= _maxChannels) {
                    if (_verbose) {
                        System.out.println("Thread " + Thread.currentThread().getId() + ": reach max session limit "
                                + _maxChannels + ". Waiting...");
                    }
                    try {
                        _channels.wait();
                    } catch (Throwable e) {
                        throw new IOException(e);
                    }
                }
            }
            com.jcraft.jsch.Channel channel = _jschSession.openChannel(type);
            _channels.add(channel);
            return channel;
        }
    }

    void removeChannel(com.jcraft.jsch.Channel channel) {
        synchronized (_channels) {
            _channels.remove(channel);
            _channels.notifyAll();
        }
    }

    private UserInfo userInfo() {
        return new UserInfo() {
            @Override
            public String getPassphrase() {
                return _cxnDetails.passphrase();
            }

            @Override
            public String getPassword() {
                return _cxnDetails.password();
            }

            @Override
            public boolean promptPassphrase(String message) {
                return _cxnDetails.passphrase() != null;
            }

            @Override
            public boolean promptPassword(String message) {
                return _cxnDetails.password() != null;
            }

            @Override
            public boolean promptYesNo(String message) {
                return false;
            }

            @Override
            public void showMessage(String message) {

            }
        };
    }

    @Override
    public boolean verbose() {
        return _verbose;
    }

}
