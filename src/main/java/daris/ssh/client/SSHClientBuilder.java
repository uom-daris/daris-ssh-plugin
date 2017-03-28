package daris.ssh.client;

import daris.ssh.client.ganymed.GanymedClient;

public abstract class SSHClientBuilder {

    private String _host;
    private int _port;
    private String _hostPubkey;
    private String _username;
    private String _password;
    private String _privateKey;
    private String _passphrase;

    public SSHClientBuilder setHost(String host) {
        _host = host;
        return this;
    }

    public SSHClientBuilder setPort(int port) {
        _port = port;
        return this;
    }

    public SSHClientBuilder setHostKey(String hostPubkey) {
        _hostPubkey = hostPubkey;
        return this;
    }

    public SSHClientBuilder setUsername(String username) {
        _username = username;
        return this;
    }

    public SSHClientBuilder setPassword(String password) {
        _password = password;
        return this;
    }

    public SSHClientBuilder setPrivateKey(String privateKey, String passphrase) {
        _privateKey = privateKey;
        _passphrase = passphrase;
        return this;
    }

    public SSHClient build() throws Throwable {
        return build(_host, _port, _hostPubkey, _username, _password, _privateKey, _passphrase);
    }

    protected abstract SSHClient build(String host, int port, String hostPubkey, String username, String password,
            String privateKey, String passphrase) throws Throwable;

    public static SSHClientBuilder getInstance() {
        return new GanymedClient.Builder();
    }

}
