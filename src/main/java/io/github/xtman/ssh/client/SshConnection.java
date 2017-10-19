package io.github.xtman.ssh.client;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.ServerHostKeyVerifier;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.channel.Channel;
import io.github.xtman.util.AbortCheck;

public class SshConnection implements Closeable {

    public static final int DEFAULT_MAX_SESSIONS = 0;

    public static final int CONNECT_TIMEOUT_MILLISECS = 60000; // 60 seconds

    public static final int KEY_EXCHANGE_TIMEOUT_MILLISECS = 60000; // 60
                                                                    // seconds

    public static final int EXEC_TIMEOUT_MILLISECS = 5000; // 5 seconds

    private String _host;
    private int _port;
    private String _hostPublicKey;
    private String _username;
    private String _password;
    private String _userPrivateKey;
    private String _userPrivateKeyPassphrase;

    private Connection _cxn;
    private ConnectionInfo _cxnInfo;

    private int _maxSessions;
    private List<Session> _sessions;

    private boolean _verbose = false;

    SshConnection(String host, int port, String hostPublicKey, String username, String password, String userPrivateKey,
            String userPrivateKeyPassphrase, int maxSessions) throws Throwable {
        _host = host;
        _port = port;
        _hostPublicKey = hostPublicKey;
        _username = username;
        _password = password;
        _userPrivateKey = userPrivateKey;
        _userPrivateKeyPassphrase = userPrivateKeyPassphrase;

        _maxSessions = maxSessions;
        _sessions = Collections.synchronizedList(new ArrayList<Session>());

        _cxn = new ch.ethz.ssh2.Connection(_host, _port) {

            public Session openSession() throws IOException {
                synchronized (_sessions) {
                    if (_maxSessions > 0) {
                        while (_sessions.size() >= _maxSessions) {
                            if (_verbose) {
                                System.out.println("Thread " + Thread.currentThread().getId()
                                        + ": reach max session limit " + _maxSessions + ". Waiting...");
                            }
                            try {
                                _sessions.wait();
                            } catch (Throwable e) {
                                throw new IOException(e);
                            }
                        }
                    }
                    Session session = super.openSession();
                    _sessions.add(session);
                    if (_verbose) {
                        System.out.println("Thread " + Thread.currentThread().getId() + ": session created.");
                    }
                    return session;
                }
            }
        };
        _cxn.setServerHostKeyAlgorithms(new String[] { "ssh-rsa" });
        _cxnInfo = _cxn.connect(_hostPublicKey == null ? null : new ServerHostKeyVerifier() {

            @Override
            public boolean verifyServerHostKey(String hostname, int port, String serverHostKeyType,
                    byte[] serverHostKey) throws Exception {
                if (_hostPublicKey == null) {
                    return true;
                } else {
                    byte[] hostKey = SshKeyTools.getPublicKeyBytes(_hostPublicKey);
                    String hostKeyType = SshKeyTools.getPublicKeyType(hostKey);
                    if (!serverHostKeyType.equals(hostKeyType)) {
                        return false;
                    }
                    return Arrays.equals(hostKey, serverHostKey);
                }
            }
        }, CONNECT_TIMEOUT_MILLISECS, KEY_EXCHANGE_TIMEOUT_MILLISECS);
        try {
            if (_password == null && _userPrivateKey == null) {
                throw new IllegalArgumentException("Missing user's password or private key.");
            }
            boolean authenticated = false;
            if (_password != null) {
                authenticated = _cxn.authenticateWithPassword(_username, _password);
            }
            if (!authenticated && _userPrivateKey != null) {
                authenticated = _cxn.authenticateWithPublicKey(_username, _userPrivateKey.toCharArray(),
                        _userPrivateKeyPassphrase);
            }
            if (!authenticated) {
                throw new Exception("Failed to authenticate user: " + _username);
            }
        } catch (Throwable e) {
            if (_cxn != null) {
                try {
                    _cxn.close();
                } catch (Throwable t) {

                }
            }
            throw e;
        }
    }

    public String host() {
        return _host;
    }

    public int port() {
        return _port;
    }

    public String username() {
        return _username;
    }

    public String password() {
        return _password;
    }

    void removeSession(Session session) {
        synchronized (_sessions) {
            _sessions.remove(session);
            _sessions.notifyAll();
        }
    }

    void removeClosedSessions() {
        synchronized (_sessions) {
            for (Iterator<Session> it = _sessions.iterator(); it.hasNext();) {
                Session session = it.next();
                if (session.getState() == Channel.STATE_CLOSED) {
                    it.remove();
                    _sessions.notifyAll();
                }
            }
        }
    }

    public SshSession createSession() throws Throwable {
        return new SshSession(this, _cxn.openSession());
    }

    public ScpPutClient createScpPutClient(String encoding, String baseDir, boolean preserveMtime, boolean compress,
            int dirMode, int fileMode) throws Throwable {
        return new ScpPutClient(this, _cxn.openSession(), encoding, baseDir, preserveMtime, compress, dirMode, fileMode,
                false);
    }

    public ScpPutClient createScpPutClient(String baseDir) throws Throwable {
        return new ScpPutClient(this, _cxn.openSession(), baseDir);
    }

    public ScpPutClient createScpPutClient() throws Throwable {
        return new ScpPutClient(this, _cxn.openSession());
    }

    public int exec(String command, String charsetName, OutputStream stdout, OutputStream stderr, AbortCheck abortCheck)
            throws Throwable {
        Session session = _cxn.openSession();
        try {
            session.execCommand(command, charsetName);
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
                                    // TODO: this may not be right since some
                                    // server does not send exit status. exit
                                    // status is available
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

    public SimpleEntry<Integer, String[]> exec(String command, String charsetName) throws Throwable {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitStatus = exec(command, charsetName, out, err, null);
        return new SimpleEntry<Integer, String[]>(exitStatus,
                new String[] { charsetName == null ? out.toString() : out.toString(charsetName),
                        charsetName == null ? err.toString() : err.toString(charsetName) });
    }

    public int exec(String command) throws Throwable {
        return exec(command, null, null, null, null);
    }

    public SftpClient createSftpClient() throws IOException {
        return new SftpClient(this, new SFTPv3Client(_cxn));
    }

    public SftpClient createSftpClient(String baseDir) throws IOException {
        return new SftpClient(this, new SFTPv3Client(_cxn), baseDir);
    }

    public SftpClient createSftpClient(String baseDir, int dirMode, int fileMode) throws Throwable {
        return new SftpClient(this, new SFTPv3Client(_cxn), baseDir, dirMode, fileMode);
    }

    private void clearSessions() {
        synchronized (_sessions) {
            for (Session session : _sessions) {
                try {
                    if (session.getState() != Channel.STATE_CLOSED) {
                        session.close();
                    }
                } catch (Throwable e) {

                } finally {
                    _sessions.remove(session);
                    _sessions.notifyAll();
                }
            }
        }
    }

    protected ConnectionInfo connectionInfo() {
        return _cxnInfo;
    }

    public void close() throws IOException {
        try {
            clearSessions();
        } finally {
            if (_cxn != null) {
                _cxn.close();
                if (_verbose) {
                    System.out.println("Thread " + Thread.currentThread().getId() + ": closed ssh connection.");
                }
            }
        }
    }

    public static SshConnection create(String host, int port, String hostPublicKey, String username, String password)
            throws Throwable {
        return new SshConnection(host, port, hostPublicKey, username, password, null, null, DEFAULT_MAX_SESSIONS);
    }

    public static SshConnection create(String host, int port, String hostPublicKey, String username, String password,
            int maxSessions) throws Throwable {
        return new SshConnection(host, port, hostPublicKey, username, password, null, null, maxSessions);
    }

    public static SshConnection create(String host, int port, String hostPublicKey, String username,
            String userPrivateKey, String userPrivateKeyPassphrase) throws Throwable {
        return new SshConnection(host, port, hostPublicKey, username, null, userPrivateKey, userPrivateKeyPassphrase,
                DEFAULT_MAX_SESSIONS);
    }

    public static SshConnection create(String host, int port, String hostPublicKey, String username,
            String userPrivateKey, String userPrivateKeyPassphrase, int maxSessions) throws Throwable {
        return new SshConnection(host, port, hostPublicKey, username, null, userPrivateKey, userPrivateKeyPassphrase,
                maxSessions);
    }

}
