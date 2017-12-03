package io.github.xtman.ssh.client;

import java.util.Base64;

public abstract class KeyTools {

//    public String getServerHostKey(String host, int port, KeyType type) throws Throwable {
//        byte[] b = getServerHostKey(type, host, port);
//        if (b != null) {
//            return new String(Base64.getEncoder().encode(b), "UTF-8");
//        }
//        return null;
//    }
    
    public abstract String getServerHostKey(String host, int port, KeyType type) throws Throwable;

    public abstract byte[] getServerHostKey(KeyType type, String host, int port) throws Throwable;

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

    public static String getPublicKeyPrefix(String publicKey) {
        byte[] bytes = getPublicKeyBytes(publicKey);
        if (bytes == null) {
            return null;
        }
        int len = ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
        return new String(bytes, 4, len);
    }

    public static KeyType getPublicKeyType(String publicKey) {
        return KeyType.fromPublicKey(publicKey);
    }

    public static KeyType getPrivateKeyType(String privateKey) {
        return KeyType.fromPrivateKey(privateKey);
    }

    // public static void main(String[] args) throws Throwable {
    // String s1 =
    // "AAAAC3NzaC1lZDI1NTE5AAAAIHlVfsR9Qso63kag4JCet5tpjRTmk5z5sEu9RfWuAgp/";
    // String s2 = "ssh-ed25519
    // AAAAC3NzaC1lZDI1NTE5AAAAIHlVfsR9Qso63kag4JCet5tpjRTmk5z5sEu9RfWuAgp/";
    // System.out.println(getPublicKeyPrefix(s1));
    // System.out.println(getPublicKeyPrefix(s2));
    // }

}
