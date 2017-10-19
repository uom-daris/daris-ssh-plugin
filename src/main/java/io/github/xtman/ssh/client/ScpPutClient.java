package io.github.xtman.ssh.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import ch.ethz.ssh2.Session;
import io.github.xtman.io.util.StreamUtils;
import io.github.xtman.util.PathUtils;

public class ScpPutClient implements Closeable {

    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final int DEFAULT_DIRECTORY_MODE = 0755;
    public static final int DEFAULT_FILE_MODE = 0644;
    public static final String DEFAULT_BASE_DIRECTORY = ".";

    private SshConnection _connection;
    private Session _session;
    private String _encoding;
    private String _baseDir;
    private boolean _preserveMtime;
    private boolean _compress;
    private boolean _verbose;
    private int _dirMode;
    private int _fileMode;

    private boolean _initialized = false;

    private InputStream _cin;
    private OutputStream _cout;

    ScpPutClient(SshConnection connection, Session session) {
        this(connection, session, DEFAULT_ENCODING, DEFAULT_BASE_DIRECTORY, false, false, DEFAULT_DIRECTORY_MODE,
                DEFAULT_FILE_MODE, false);
    }

    ScpPutClient(SshConnection connection, Session session, String baseDir) {
        this(connection, session, DEFAULT_ENCODING, baseDir, false, false, DEFAULT_DIRECTORY_MODE, DEFAULT_FILE_MODE,
                false);
    }

    ScpPutClient(SshConnection connection, Session session, String encoding, String baseDir, boolean preserveMtime,
            boolean compress, int dirMode, int fileMode) {
        this(connection, session, encoding, baseDir, preserveMtime, compress, dirMode, fileMode, false);
    }

    ScpPutClient(SshConnection connection, Session session, String encoding, String baseDir, boolean preserveMtime,
            boolean compress, int dirMode, int fileMode, boolean verbose) {
        _connection = connection;
        _session = session;
        _encoding = encoding == null ? DEFAULT_ENCODING : encoding;
        setBaseDirectory(baseDir);
        _preserveMtime = preserveMtime;
        _compress = compress;
        _dirMode = dirMode < 0 ? DEFAULT_DIRECTORY_MODE : dirMode;
        _fileMode = fileMode < 0 ? DEFAULT_FILE_MODE : fileMode;
        _verbose = verbose;

        _initialized = false;
    }

    private void setBaseDirectory(String baseDir) {
        if (baseDir == null || baseDir.trim().isEmpty()) {
            _baseDir = DEFAULT_BASE_DIRECTORY;
        } else {
            baseDir = baseDir.trim();
            if (baseDir.startsWith("/") || baseDir.startsWith(".")) {
                _baseDir = baseDir;
            } else {
                _baseDir = PathUtils.join(".", baseDir);
            }
        }
    }

    public String encoding() {
        return _encoding;
    }

    public String baseDirectory() {
        return _baseDir;
    }

    public boolean preserveMtime() {
        return _preserveMtime;
    }

    public boolean compress() {
        return _compress;
    }

    public int directoryMode() {
        return _dirMode;
    }

    public String directoryModeToString() {
        return String.format("%04o", _dirMode);
    }

    public int fileMode() {
        return _fileMode;
    }

    public String fileModeToString() {
        return String.format("%04o", _fileMode);
    }

    public boolean verbose() {
        return _verbose;
    }

    private void initialize() throws IOException {

        if (_initialized) {
            return;
        }

        _cin = new BufferedInputStream(_session.getStdout(), 8192);
        _cout = new BufferedOutputStream(_session.getStdin());

        // @formatter:off
        // -r    recursive 
        // -d    target should be a directory
        // -t    ???
        // -p    preserve mtime
        // -C    compress
        // @formatter:on

        StringBuilder sb = new StringBuilder("scp -r -d -t ");
        if (preserveMtime()) {
            sb.append("-p ");
        }
        if (compress()) {
            sb.append("-C ");
        }
        sb.append(baseDirectory()).append("\n");
        String cmd = sb.toString();

        /*
         * go to base dir
         */
        if (_verbose) {
            System.out.println("Sending command: " + cmd);
        }
        _session.execCommand(cmd);
        checkResponse(_cin);

        _initialized = true;
    }

    /**
     * Scp the source file to the remote destination path.
     * 
     * @param in
     *            the input stream of the source file.
     * @param length
     *            the length of the source file.
     * @param dstPath
     *            the destination relative path (to the base directory)
     * @throws Throwable
     */
    public void put(InputStream in, long length, String dstPath) throws Throwable {
        put(in, length, -1, dstPath);
    }

    /**
     * Scp the source file to the remote destination path.
     * 
     * @param in
     *            the input stream of the source file.
     * @param length
     *            the length of the source file.
     * @param lastModified
     *            the time that the file was last modified (aka. mtime).
     * @param dstPath
     *            the destination relative path (to the base directory)
     * @throws Throwable
     */
    public void put(InputStream in, long length, long lastModified, String dstPath) throws Throwable {

        /*
         * make sure it is initialized
         */
        initialize();

        /*
         * enter directory
         */
        dstPath = PathUtils.trimSlash(dstPath);
        String[] components = dstPath.split("/");
        if (components.length > 1) {
            for (int i = 0; i < components.length - 1; i++) {
                String p = components[i];
                pushDir(p);
            }
        }
        /*
         * send file
         */
        if (preserveMtime() && lastModified > 0) {
            // send mtime
            long mtime = lastModified / 1000;
            String cmd = String.format("T%d 0 %d 0\n", mtime, mtime);

            if (_verbose) {
                System.out.println("Sending command: " + cmd);
            }
            _cout.write(cmd.getBytes(encoding()));
            _cout.flush();
            checkResponse(_cin);
        }
        // send file mode, length and name
        String fileName = PathUtils.getLastComponent(dstPath);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("C%04o %d %s\n", fileMode(), length, fileName));
        String cmd = sb.toString();

        if (_verbose) {
            System.out.println("Sending command: " + cmd);
        }
        _cout.write(cmd.getBytes(encoding()));
        _cout.flush();
        checkResponse(_cin);

        // send file content
        StreamUtils.copy(in, length, _cout);
        // _cout.flush();
        _cout.write(new byte[] { 0 });
        _cout.flush();

        /*
         * back to base dir
         */
        if (components.length > 1) {
            for (int i = 0; i < components.length - 1; i++) {
                popDir();
            }
        }
    }

    public void put(File file, String dstPath) throws Throwable {
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        try {
            put(in, file.length(), file.lastModified(), dstPath);
        } finally {
            in.close();
        }
    }

    public void put(Path path, String dstPath) throws Throwable {
        put(path.toFile(), dstPath);
    }

    public void putDirectory(File dir) throws Throwable {
        putDirectory(dir, true);
    }

    public void putDirectory(File dir, boolean self) throws Throwable {
        putDirectory(dir.toPath(), self);
    }

    public void putDirectory(Path dir) throws Throwable {
        putDirectory(dir, true);
    }

    public void putDirectory(final Path dir, boolean self) throws Throwable {

        initialize();

        if (self) {
            pushDir(dir.getFileName().toString());
        }

        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                String dstPath = PathUtils.getRelativePath(file, dir);
                try {
                    put(file, dstPath);
                } catch (Throwable e) {
                    if (e instanceof IOException) {
                        throw (IOException) e;
                    } else {
                        throw new IOException(e);
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException ioe) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException ioe) {
                return FileVisitResult.CONTINUE;
            }
        });
        if (self) {
            popDir();
        }
    }

    /**
     * Enter the specified sub-directory.
     * 
     * @param dirName
     *            The name of the sub-directory.
     * @throws IOException
     */
    private void pushDir(String dirName) throws IOException {
        String cmd = String.format("D%04o 0 %s\n", directoryMode(), dirName);
        if (_verbose) {
            System.out.println("Sending command: " + cmd);
        }
        _cout.write(cmd.getBytes(encoding()));
        _cout.flush();
        checkResponse(_cin);
    }

    /**
     * Go back to parent directory. (cd ..)
     * 
     * @throws IOException
     */
    private void popDir() throws IOException {
        String cmd = "E\n";
        if (_verbose) {
            System.out.println("Sending command: " + cmd);
        }
        _cout.write(cmd.getBytes(encoding()));
        _cout.flush();
        checkResponse(_cin);
    }

    private static void checkResponse(InputStream in) throws IOException {
        final int b = in.read();
        // b may be
        // 0 for success,
        // 1 for error,
        // 2 for fatal error,

        if (b == -1) {
            // didn't receive any response
            throw new IOException("No response from remote server.");
        } else if (b != 0) {
            StringBuffer sb = new StringBuffer();
            int c = in.read();
            while (c > 0 && c != '\n') {
                sb.append((char) c);
                c = in.read();
            }
            if (b == 1) {
                throw new IOException("Remote server responded error(" + b + "): " + sb.toString());
            } else if (b == 2) {
                throw new IOException("Remote server responded fatal error(" + b + "): " + sb.toString());
            } else {
                throw new IOException("Remote server responded unknown error(" + b + "): " + sb.toString());
            }
        }

    }

    public void mkdirs(String dstPath) throws IOException {
        initialize();
        if (dstPath != null && !dstPath.trim().isEmpty()) {
            dstPath = PathUtils.normalise(dstPath);
            dstPath = PathUtils.trimLeadingSlash(dstPath);
            String[] cs = dstPath.split("/");
            for (int i = 0; i < cs.length; i++) {
                pushDir(cs[i]);
            }
            for (int i = 0; i < cs.length; i++) {
                popDir();
            }
        }
    }

    public void close() throws IOException {
        if (_session != null) {
            try {
                _session.getStderr().close();
            } catch (Throwable e) {

            }
            try {
                _session.getStdin().close();
            } catch (Throwable e) {

            }
            try {
                _session.getStdout().close();
            } catch (Throwable e) {

            }
            try {
                _session.close();
                if (_verbose) {
                    System.out.println("Thread " + Thread.currentThread().getId() + ": closed scp.put session.");
                }
            } finally {
                _connection.removeSession(_session);
            }
        }
    }

    public SshConnection connection() {
        return _connection;
    }

}
