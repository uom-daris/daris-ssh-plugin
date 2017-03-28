package daris.ssh.client;

import java.util.Base64;

public class KeyTools {

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
