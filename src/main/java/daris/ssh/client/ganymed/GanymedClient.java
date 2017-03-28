package daris.ssh.client.ganymed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Stack;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.SCPOutputStream;
import ch.ethz.ssh2.SFTPOutputStream;
import ch.ethz.ssh2.SFTPv3FileHandle;
import ch.ethz.ssh2.ServerHostKeyVerifier;
import daris.io.StreamUtils;
import daris.ssh.client.KeyTools;
import daris.ssh.client.SSHClient;
import daris.ssh.client.SSHClientBuilder;
import daris.util.AbortCheck;
import daris.util.PathUtils;

public class GanymedClient implements SSHClient {

    public static class Builder extends SSHClientBuilder {

        @Override
        protected SSHClient build(String host, int port, String hostPubkey, String username, String password,
                String privateKey, String passphrase) throws Throwable {
            return new GanymedClient(host, port, hostPubkey, username, password, privateKey, passphrase);
        }

    }

    public static final int CONNECT_TIMEOUT_MILLISECS = 60000; // 60 seconds

    public static final int KEY_EXCHANGE_TIMEOUT_MILLISECS = 60000; // 60
                                                                    // seconds

    public static final int EXEC_TIMEOUT_MILLISECS = 5000; // 5 seconds

    private String _host;
    private int _port;
    private String _hostPublicKey;
    private String _username;
    private String _password;
    private String _privateKey;
    private String _passphrase;

    private ch.ethz.ssh2.Connection _conn;
    private ch.ethz.ssh2.ConnectionInfo _connInfo;
    private ch.ethz.ssh2.SFTPv3Client _sftpClient;

    public GanymedClient(String host, int port, String hostPublicKey, String username, String password,
            String privateKey, String passphrase) throws Throwable {
        _host = host;
        _port = port;
        _hostPublicKey = hostPublicKey;
        _username = username;
        _password = password;
        _privateKey = privateKey;
        _passphrase = passphrase;
        connect();
    }

    private void connect() throws Throwable {
        if (_conn == null) {
            _conn = new ch.ethz.ssh2.Connection(_host, _port);
            _conn.setServerHostKeyAlgorithms(new String[] { "ssh-rsa", "ssh-dss" });
        }
        if (_connInfo == null) {
            _connInfo = _conn.connect(_hostPublicKey == null ? null : new ServerHostKeyVerifier() {

                @Override
                public boolean verifyServerHostKey(String hostname, int port, String serverHostKeyType,
                        byte[] serverHostKey) throws Exception {
                    if (_hostPublicKey == null) {
                        return true;
                    } else {
                        byte[] hostKey = KeyTools.getPublicKeyBytes(_hostPublicKey);
                        String hostKeyType = KeyTools.getPublicKeyType(hostKey);
                        if (!serverHostKeyType.equals(hostKeyType)) {
                            return false;
                        }
                        return Arrays.equals(hostKey, serverHostKey);
                    }
                }
            }, CONNECT_TIMEOUT_MILLISECS, KEY_EXCHANGE_TIMEOUT_MILLISECS);
            boolean authenticated = false;
            if (_password != null) {
                authenticated = _conn.authenticateWithPassword(_username, _password);
            }
            if (!authenticated && _privateKey != null) {
                authenticated = _conn.authenticateWithPublicKey(_username, _privateKey.toCharArray(), _passphrase);
            }
            if (!authenticated) {
                throw new Exception("Failed to authenticate user: " + _username);
            }
        }
        if (_sftpClient != null) {
            // reconnect sftp channel
            if (!_sftpClient.isConnected()) {
                try {
                    _sftpClient.close();
                } catch (Throwable ex) {
                    ex.printStackTrace(System.err);
                }
                _sftpClient = new ch.ethz.ssh2.SFTPv3Client(_conn);
            }
        }
    }

    private void disconnect() throws Throwable {
        try {
            if (_sftpClient != null) {
                try {
                    _sftpClient.close();
                } finally {
                    _sftpClient = null;
                }
            }
        } finally {
            if (_conn != null) {
                try {
                    _conn.close();
                } finally {
                    _conn = null;
                    _connInfo = null;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            disconnect();
        } catch (Throwable e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void scpPut(InputStream in, long length, String remoteFilePath) throws Throwable {
        connect();
        ch.ethz.ssh2.SCPClient scpClient = _conn.createSCPClient();
        String remoteDirPath = PathUtils.getParent(remoteFilePath);
        if (remoteDirPath != null) {
            mkdirs(remoteDirPath);
        }
        String remoteFileName = PathUtils.getLastComponent(remoteFilePath);
        SCPOutputStream out = scpClient.put(remoteFileName, length, remoteDirPath, "0644");
        try {
            StreamUtils.copy(in, out);
        } finally {
            out.close();
        }
    }

    @Override
    public void sftpPut(InputStream in, long length, String remoteFilePath) throws Throwable {
        connect();
        if (_sftpClient == null) {
            _sftpClient = new ch.ethz.ssh2.SFTPv3Client(_conn);
        }
        String remoteDirPath = PathUtils.getParent(remoteFilePath);
        if (remoteDirPath != null) {
            mkdirs(_sftpClient, remoteDirPath);
        }
        SFTPv3FileHandle f = _sftpClient.createFile(remoteFilePath);
        try {
            SFTPOutputStream out = new SFTPOutputStream(f);
            try {
                StreamUtils.copy(in, out);
            } finally {
                out.close();
            }
        } finally {
            _sftpClient.closeFile(f);
        }
    }

    @Override
    public void mkdirs(String remoteDirPath) throws Throwable {
        connect();
        if (_sftpClient != null && _sftpClient.isConnected()) {
            mkdirs(_sftpClient, remoteDirPath);
        } else {
            mkdirs(_conn, remoteDirPath);
        }
    }

    public int execute(String command, OutputStream stdout, OutputStream stderr, AbortCheck abortCheck)
            throws Throwable {
        connect();
        return execute(_conn, command, stdout, stderr, abortCheck);
    }

    static boolean directoryExists(ch.ethz.ssh2.SFTPv3Client sftpClient, String remoteDirPath) throws Throwable {
        SFTPv3FileHandle dir = null;
        try {
            dir = sftpClient.openDirectory(remoteDirPath);
            return dir != null;
        } catch (ch.ethz.ssh2.SFTPException ex) {
            if (ex.getMessage().indexOf("SSH_FX_NO_SUCH_FILE") >= 0) {
                return false;
            } else {
                throw ex;
            }
        } finally {
            if (dir != null) {
                sftpClient.closeFile(dir);
            }
        }
    }

    static String mkdirs(ch.ethz.ssh2.SFTPv3Client sftpClient, String remoteDirPath) throws Throwable {
        if (directoryExists(sftpClient, remoteDirPath)) {
            return sftpClient.canonicalPath(remoteDirPath);
        }
        Stack<String> stack = new Stack<String>();
        stack.push(remoteDirPath);
        for (String parent = PathUtils.getParent(remoteDirPath); parent != null
                && !directoryExists(sftpClient, parent); parent = PathUtils.getParent(parent)) {
            stack.push(parent);
        }
        while (!stack.isEmpty()) {
            String dir = stack.pop();
            sftpClient.mkdir(dir, 0755);
        }
        return sftpClient.canonicalPath(remoteDirPath);
    }

    static String mkdirs(ch.ethz.ssh2.Connection conn, String remoteDirPath) throws Throwable {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try {
            String command = "mkdir -p -m 755 " + remoteDirPath + "; cd " + remoteDirPath + "; pwd; cd";
            int exitCode = execute(conn, command, out, err, null);
            String output = new String(out.toByteArray());
            String errMsg = new String(err.toByteArray());
            if (!errMsg.isEmpty()) {
                System.err.println("Error making directory on remote SSH server: " + errMsg);
            }
            if (exitCode != 0) {
                StringBuilder ex = new StringBuilder("Remote command exit status: " + exitCode + ". ");
                if (!errMsg.isEmpty()) {
                    ex.append("Error from remote SSH server: " + errMsg);
                }
                throw new Exception(ex.toString());
            }
            return output;
        } finally {
            out.close();
            err.close();
        }
    }

    static int execute(ch.ethz.ssh2.Connection conn, String command, OutputStream stdout, OutputStream stderr,
            AbortCheck abortCheck) throws Throwable {

        ch.ethz.ssh2.Session session = conn.openSession();
        try {
            session.execCommand(command);
            InputStream remoteStdout = session.getStdout();
            InputStream remoteStderr = session.getStderr();
            byte[] buffer = new byte[1024];
            int exitStatus = 0;
            try {
                while (true) {
                    if (abortCheck != null && abortCheck.aborted()) {
                        break;
                    }
                    if (remoteStderr.available() > 0) {
                        exitStatus = 1;
                    }
                    if ((remoteStdout.available() == 0) && (remoteStderr.available() == 0)) {

                        /*
                         * Even though currently there is no data available, it
                         * may be that new data arrives and the session's
                         * underlying channel is closed before we call
                         * waitForCondition(). This means that EOF and
                         * STDOUT_DATA (or STDERR_DATA, or both) may be set
                         * together.
                         */
                        int conditions = session.waitForCondition(
                                ChannelCondition.STDOUT_DATA | ChannelCondition.STDERR_DATA | ChannelCondition.EOF,
                                EXEC_TIMEOUT_MILLISECS);

                        /*
                         * Wait no longer than 5 seconds (= 5000 milliseconds)
                         */

                        if ((conditions & ChannelCondition.TIMEOUT) != 0) {
                            /* A timeout occured. */
                            throw new IOException("Timeout while waiting for data from peer.");
                        }

                        /*
                         * Here we do not need to check separately for CLOSED,
                         * since CLOSED implies EOF
                         */
                        if ((conditions & ChannelCondition.EOF) != 0) {
                            /* The remote side won't send us further data... */
                            if ((conditions & (ChannelCondition.STDOUT_DATA | ChannelCondition.STDERR_DATA)) == 0) {
                                /*
                                 * ... and we have consumed all data in the
                                 * local arrival window.
                                 */
                                if ((conditions & ChannelCondition.EXIT_STATUS) != 0) {
                                    // NOTE: this may not be right since some
                                    // server does not send exit status.
                                    // exit status is available
                                    if (session.getExitStatus() != null) {
                                        exitStatus = session.getExitStatus();
                                    }
                                    break;
                                }
                            }
                        }

                        /*
                         * OK, either STDOUT_DATA or STDERR_DATA (or both) is
                         * set.
                         */

                        // You can be paranoid and check that the library is not
                        // going nuts:
                        // if ((conditions & (ChannelCondition.STDOUT_DATA |
                        // ChannelCondition.STDERR_DATA)) == 0)
                        // throw new IllegalStateException("Unexpected condition
                        // result (" + conditions + ")");
                    }

                    /*
                     * If you below replace "while" with "if", then the way the
                     * output appears on the local stdout and stder streams is
                     * more "balanced". Addtionally reducing the buffer size
                     * will also improve the interleaving, but performance will
                     * slightly suffer. OKOK, that all matters only if you get
                     * HUGE amounts of stdout and stderr data =)
                     */
                    while (remoteStdout.available() > 0) {
                        int i = remoteStdout.read(buffer);
                        if (i < 0) {
                            break;
                        }
                        if (stdout != null) {
                            stdout.write(buffer, 0, i);
                        }
                    }
                    while (remoteStderr.available() > 0) {
                        int i = remoteStderr.read(buffer);
                        if (i < 0) {
                            break;
                        }
                        if (stderr != null) {
                            stderr.write(buffer, 0, i);
                        }
                    }
                }
            } finally {
                remoteStdout.close();
                remoteStderr.close();
            }
            return exitStatus;
        } finally {
            session.close();
        }
    }

    public static byte[] getServerHostKey(String host, int port) throws Throwable {
        final byte[][] b = new byte[1][];
        ch.ethz.ssh2.Connection _conn = new ch.ethz.ssh2.Connection(host, port);
        try {
            _conn.setServerHostKeyAlgorithms(new String[] { "ssh-rsa", "ssh-dss" });
            _conn.connect(new ServerHostKeyVerifier() {

                @Override
                public boolean verifyServerHostKey(String hostname, int port, String serverHostKeyType,
                        byte[] serverHostKey) throws Exception {
                    b[0] = serverHostKey;
                    return true;
                }
            }, CONNECT_TIMEOUT_MILLISECS, KEY_EXCHANGE_TIMEOUT_MILLISECS);
            return b[0];
        } finally {
            _conn.close();
        }
    }

}
