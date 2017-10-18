package io.github.xtman.ssh.client;

import java.util.Base64;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;

public class SshKeyTools {

    public static String getServerHostKey(String host, int port, String... algorithms) throws Throwable {
        byte[] key = getServerHostKeyBytes(host, port, algorithms);
        return new String(Base64.getEncoder().encode(key), "UTF-8");
    }

    public static String getServerHostKey(String host, int port) throws Throwable {
        return getServerHostKey(host, port, "ssh-rsa");
    }

    public static byte[] getServerHostKeyBytes(String host, int port, String... algorithms) throws Throwable {
        Connection cxn = new ch.ethz.ssh2.Connection(host, port);
        cxn.setServerHostKeyAlgorithms(algorithms);
        try {
            ConnectionInfo cxnInfo = cxn.connect(null, SshConnection.CONNECT_TIMEOUT_MILLISECS,
                    SshConnection.KEY_EXCHANGE_TIMEOUT_MILLISECS);
            return cxnInfo.serverHostKey;
        } finally {
            cxn.close();
        }
    }

    public static byte[] getServerHostKeyBytes(String host, int port) throws Throwable {
        return getServerHostKeyBytes(host, port, "ssh-rsa");
    }
    
    
    public static byte[] getPublicKeyBytes(String publicKey) {
        if (publicKey == null) {
            return null;
        }
        String key = publicKey.trim();
        int idx = key.indexOf("ssh-");
        if (idx != -1) {
            key = key.substring(idx);
            key = key.substring(key.indexOf(' ')).trim();
        }
        idx = key.indexOf(' ');
        if (idx != -1) {
            key = key.substring(0, idx);
        }
        return Base64.getDecoder().decode(key);
    }

    public static String getPublicKeyType(byte[] publicKeyBytes) {
        if (publicKeyBytes == null) {
            return null;
        }
        int len = ((publicKeyBytes[0] & 0xff) << 24) | ((publicKeyBytes[1] & 0xff) << 16)
                | ((publicKeyBytes[2] & 0xff) << 8) | (publicKeyBytes[3] & 0xff);
        return new String(publicKeyBytes, 4, len);
    }

    public static String getPublicKeyType(String publicKey) {
        byte[] bytes = getPublicKeyBytes(publicKey);
        if (bytes != null) {
            return getPublicKeyType(bytes);
        }
        return null;
    }

}
