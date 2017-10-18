package daris.ssh.plugin.sink;

import java.io.IOException;
import java.io.InputStream;

import io.github.xtman.ssh.client.SftpClient;
import io.github.xtman.ssh.client.SshConnection;

public class SFTPSink extends SSHSink {

    public static final String TYPE_NAME = "daris-sftp";

    public SFTPSink() throws Throwable {
        super(TYPE_NAME);
    }

    @Override
    public String description() throws Throwable {
        return "SFTP sink.";
    }

    @Override
    protected Client createClient(String host, int port, String hostKey, String username, String password,
            String privateKey, String passphrase, String directory, int dirMode, int fileMode) throws Throwable {
        return new ClientWrapper(host, port, hostKey, username, password, privateKey, passphrase, directory, dirMode,
                fileMode);
    }

    private static class ClientWrapper implements Client {

        private SshConnection _cxn;
        private SftpClient _sftp;

        ClientWrapper(String host, int port, String hostKey, String username, String password, String privateKey,
                String passphrase, String directory, int dirMode, int fileMode) throws Throwable {
            if (privateKey != null) {
                _cxn = SshConnection.create(host, port, null, username, privateKey, passphrase);
            } else if (password != null) {
                _cxn = SshConnection.create(host, port, hostKey, username, password);
            } else {
                throw new IllegalArgumentException("Missing password or private-key.");
            }
            _sftp = _cxn.createSftpClient(directory, dirMode, fileMode);
        }

        @Override
        public void close() throws IOException {
            try {
                _sftp.close();
            } finally {
                _cxn.close();
            }
        }

        @Override
        public String baseDirectory() {
            return _sftp.baseDirectory();
        }

        @Override
        public void put(InputStream in, long length, String dstPath) throws Throwable {
            _sftp.put(in, length, dstPath);
        }

        @Override
        public void mkdirs(String dstDirPath) throws Throwable {
            _sftp.mkdirs(dstDirPath);
        }

    }

}
