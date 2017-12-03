package io.github.xtman.ssh.client;

public class ConnectionDetails {

    private String _host;
    private int _port;
    private String _hostKey;
    private String _username;
    private String _password;
    private String _privateKey;
    private String _passphrase;
    private String _publicKey;

    public ConnectionDetails(String host, int port, String hostKey, String username, String password, String privateKey,
            String passphrase, String publicKey) {
        _host = host;
        _port = port;
        _hostKey = hostKey;
        _username = username;
        _password = password;
        _privateKey = privateKey;
        _passphrase = passphrase;
        _publicKey = publicKey;
    }

    public ConnectionDetails(String host, int port, String hostKey, String username, String password, String privateKey,
            String passphrase) {
        this(host, port, hostKey, username, password, privateKey, passphrase, null);
    }

    public String host() {
        return _host;
    }

    public int port() {
        return _port;
    }

    public String hostKey() {
        return _hostKey;
    }

    public String username() {
        return _username;
    }

    public String password() {
        return _password;
    }

    public String privateKey() {
        return _privateKey;
    }

    public String passphrase() {
        return _passphrase;
    }

    public String publicKey() {
        return _publicKey;
    }

}
