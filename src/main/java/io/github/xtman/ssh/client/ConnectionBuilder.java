package io.github.xtman.ssh.client;

import io.github.xtman.ssh.client.jsch.JschConnection;

public class ConnectionBuilder {

    private String _impl = "jsch";
    private String _host;
    private int _port = 22;
    private String _hostKey;
    private String _username;
    private String _password;
    private String _privateKey;
    private String _passphrase;
    private String _publicKey;
    private int _maxSessions = Connection.DEFAULT_MAX_CHANNELS;
    private boolean _verbose = false;

    public ConnectionBuilder(String impl) {
        if (impl != null) {
            setImplementation(impl);
        }
    }

    public ConnectionBuilder() {
        this("jsch");
    }

    public ConnectionBuilder setImplementation(String impl) {
        if ("jsch".equalsIgnoreCase(impl)) {
            _impl = impl.toLowerCase();
// @formatter:off
//        } else if ("ganymed".equalsIgnoreCase(impl)) {
//            _impl = impl.toLowerCase();
// @formatter:on
        } else {
            throw new UnsupportedOperationException("Unknown SSH implementation: " + impl);
        }
        return this;
    }

    public ConnectionBuilder setHost(String host) {
        _host = host;
        return this;
    }

    public ConnectionBuilder setPort(int port) {
        _port = port;
        return this;
    }

    public ConnectionBuilder setHostKey(String hostKey) {
        _hostKey = hostKey;
        return this;
    }

    public ConnectionBuilder setServer(String host, int port, String hostKey) {
        _host = host;
        _port = port;
        _hostKey = hostKey;
        return this;
    }

    public ConnectionBuilder setUsername(String username) {
        _username = username;
        return this;
    }

    public ConnectionBuilder setPassword(String password) {
        _password = password;
        return this;
    }

    public ConnectionBuilder setPrivateKey(String privateKey) {
        _privateKey = privateKey;
        return this;
    }

    public ConnectionBuilder setPassphrase(String passphrase) {
        _passphrase = passphrase;
        return this;
    }

    public ConnectionBuilder setPrivateKey(String privateKey, String passphrase) {
        _privateKey = privateKey;
        _passphrase = passphrase;
        return this;
    }

    public ConnectionBuilder setPublicKey(String publicKey) {
        _publicKey = publicKey;
        return this;
    }

    public ConnectionBuilder setUserCredentials(String username, String password) {
        _username = username;
        _password = password;
        return this;
    }

    public ConnectionBuilder setUserCredentials(String username, String privateKey, String passphrase) {
        _username = username;
        _privateKey = privateKey;
        _passphrase = passphrase;
        return this;
    }

    public ConnectionBuilder setUserCredentials(String username, String password, String privateKey,
            String passphrase) {
        _username = username;
        _password = password;
        _privateKey = privateKey;
        _passphrase = passphrase;
        return this;
    }

    public ConnectionBuilder setMaxSessions(int maxSessions) {
        _maxSessions = maxSessions;
        return this;
    }

    public ConnectionBuilder setVerbose(boolean verbose) {
        _verbose = verbose;
        return this;
    }

    public Connection build() throws Throwable {
        if (_host == null) {
            throw new IllegalArgumentException("SSH server host is not specified.");
        }
        if (_port <= 0 || _port > 65535) {
            throw new IllegalArgumentException("Invalid SSH server port: " + _port);
        }
        if (_username == null) {
            throw new IllegalArgumentException("Username is not specified.");
        }
        if (_password == null && _privateKey == null) {
            throw new IllegalArgumentException("User's password or private key must be specified.");
        }
        ConnectionDetails cxnDetails = new ConnectionDetails(_host, _port, _hostKey, _username, _password, _privateKey,
                _passphrase, _publicKey);
        if ("jsch".equalsIgnoreCase(_impl)) {
            return new JschConnection(cxnDetails, _maxSessions, _verbose);
        } else {
            throw new UnsupportedOperationException("Unknown SSH implementation: " + _impl);
        }
    }

}
