package io.github.xtman.ssh.client;

public enum KeyType {

    RSA("RSA", "ssh-rsa"), DSA("DSA", "ssh-dss"), ECDSA("EC", "ecdsa-sha2-nistp256"), ED25519("OPENSSH", "ssh-ed25519");
    private String _privateKeyTypeName;
    private String _publicKeyTypeName;

    KeyType(String privateKeyTypeName, String publicKeyTypeName) {
        _privateKeyTypeName = privateKeyTypeName;
        _publicKeyTypeName = publicKeyTypeName;
    }

    public String type() {
        return name().toLowerCase();
    }

    public String toString() {
        return name().toLowerCase();
    }

    public String privateKeyTypeName() {
        return _privateKeyTypeName;
    }

    public String publicKeyTypeName() {
        return _publicKeyTypeName;
    }

    public static KeyType fromPrivateKey(String privateKey) {
        if (privateKey != null) {
            KeyType[] vs = values();
            for (KeyType v : vs) {
                if (privateKey.indexOf("-----BEGIN " + v.privateKeyTypeName() + " PRIVATE KEY-----") != -1
                        && privateKey.indexOf("-----END " + v.privateKeyTypeName() + " PRIVATE KEY-----") != -1) {
                    return v;
                }
            }
        }
        return null;
    }

    public static KeyType fromPublicKey(String publicKey) {
        String prefix = KeyTools.getPublicKeyPrefix(publicKey);
        if (prefix != null) {
            KeyType[] vs = values();
            for (KeyType v : vs) {
                if (prefix.equals(v.publicKeyTypeName())) {
                    return v;
                }
            }
        }
        return null;
    }
}
