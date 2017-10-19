package io.github.xtman.ssh.client;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Stack;

import ch.ethz.ssh2.SFTPException;
import ch.ethz.ssh2.SFTPOutputStream;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;
import io.github.xtman.io.util.StreamUtils;
import io.github.xtman.util.PathUtils;

public class SftpClient implements Closeable {

    public static final String DEFAULT_BASE_DIRECTORY = ".";

    public static final int DEFAULT_DIRECTORY_MODE = 0755;

    public static final int DEFAULT_FILE_MODE = 0644;

    private SshConnection _connection;

    private SFTPv3Client _client;

    private String _baseDir;
    private int _dirMode = DEFAULT_DIRECTORY_MODE;
    private int _fileMode = DEFAULT_FILE_MODE;
    private boolean _verbose;

    SftpClient(SshConnection connection, SFTPv3Client client, String baseDir, int dirMode, int fileMode,
            boolean verbose) throws IOException {
        _connection = connection;
        _client = client;
        setBaseDirectory(baseDir);
        _dirMode = dirMode;
        _fileMode = fileMode;
        _verbose = verbose;
    }

    SftpClient(SshConnection connection, SFTPv3Client client, String baseDir, int dirMode, int fileMode)
            throws IOException {
        this(connection, client, baseDir, dirMode, fileMode, false);
    }

    SftpClient(SshConnection connection, SFTPv3Client client) throws IOException {
        this(connection, client, DEFAULT_BASE_DIRECTORY, DEFAULT_DIRECTORY_MODE, DEFAULT_FILE_MODE, false);
    }

    public SftpClient(SshConnection connection, SFTPv3Client client, String baseDir) throws IOException {
        this(connection, client, baseDir, DEFAULT_DIRECTORY_MODE, DEFAULT_FILE_MODE, false);
    }

    public void setVerbose(boolean verbose) {
        _verbose = verbose;
    }

    public void setFileMode(int fileMode) {
        _fileMode = fileMode;
    }

    public void setDirectoryMode(int dirMode) {
        _dirMode = dirMode;
    }

    public void setBaseDirectory(String baseDir) {
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

    public String baseDirectory() {
        return _baseDir;
    }

    public int fileMode() {
        return _fileMode;
    }

    public int directoryMode() {
        return _dirMode;
    }

    public void putDirectory(File dir) throws Throwable {
        putDirectory(dir, true);
    }

    public void putDirectory(File dir, boolean self) throws Throwable {
        putDirectory(dir.toPath(), self);
    }

    public void putDirectory(final Path dir) throws Throwable {
        putDirectory(dir, true);
    }

    /**
     * Put the source directory (recursively) to the remote server (base
     * directory).
     * 
     * @param dir
     *            The source directory.
     * @param self
     * 
     * @throws Throwable
     */
    public void putDirectory(final Path dir, final boolean self) throws Throwable {
        final String dirName = dir.getFileName().toString();
        if (self) {
            mkdirs(dirName);
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                String dstPath = PathUtils.getRelativePath(file, dir);
                if (self) {
                    dstPath = PathUtils.join(dirName, dstPath);
                }
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
    }

    /**
     * Put the input file to the specified remote destination.
     * 
     * @param file
     *            The input file.
     * @param relDstPath
     *            Destination path relative to base directory.
     * @throws Throwable
     */
    public void put(Path file, String relDstPath) throws Throwable {
        put(file.toFile(), relDstPath);
    }

    /**
     * Put the input file to the specified remote destination.
     * 
     * @param file
     *            The input file.
     * @param relDstPath
     *            Destination path relative to base directory.
     * @throws Throwable
     */
    public void put(File file, String relDstPath) throws Throwable {
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        try {
            put(in, file.length(), file.lastModified(), relDstPath);
        } finally {
            in.close();
        }
    }

    /**
     * Put the input (file) stream to the specified remote destination.
     * 
     * @param in
     *            The input stream.
     * @param length
     *            The length of the input.
     * @param relDstPath
     *            Destination path relative to base directory.
     * @throws Throwable
     */
    public void put(InputStream in, long length, String relDstPath) throws Throwable {
        put(in, length, -1, relDstPath);
    }

    /**
     * Put the input (file) stream to the specified remote destination.
     * 
     * @param in
     *            The input stream.
     * @param length
     *            The length of the input stream.
     * @param lastModified
     *            Last modified time in milliseconds.
     * @param relDstPath
     *            The destination path relative to the base directory.
     * @throws Throwable
     */
    public void put(InputStream in, long length, long lastModified, String relDstPath) throws Throwable {
        put(in, length, lastModified > 0 ? ((int) lastModified / 1000) : null, relDstPath);
    }

    /**
     * Put the input (file) stream to the specified remote destination.
     * 
     * @param in
     *            input stream.
     * @param length
     *            the length of the input stream.
     * @param mtime
     *            Unix mtime in seconds.
     * @param relDstPath
     *            Destination path relative to base directory.
     * @throws Throwable
     */
    public void put(InputStream in, long length, Integer mtime, String relDstPath) throws Throwable {
        SFTPv3FileAttributes fileAttrs = new SFTPv3FileAttributes();
        fileAttrs.mtime = mtime;
        fileAttrs.permissions = 0100000 + _fileMode;
        fileAttrs.size = length;
        put(_client, in, length, PathUtils.join(_baseDir, relDstPath), fileAttrs, _dirMode);
    }

    /**
     * Check if the remote directory exists.
     * 
     * @param relDirPath
     *            Diretory path relative to the base directory.
     * @return
     * @throws Throwable
     */
    public boolean directoryExists(String relDirPath) throws Throwable {
        return directoryExists(_client, PathUtils.join(_baseDir, relDirPath));
    }

    /**
     * Create directories recursively under the base directory.
     * 
     * @param relDirPath
     *            Relative path to the directory.
     * @return
     * @throws Throwable
     */
    public String mkdirs(String relDirPath) throws Throwable {
        return mkdirs(_client, PathUtils.join(_baseDir, relDirPath), _dirMode);
    }

    /**
     * Check if remote directory exists.
     * 
     * @param client
     * @param absDstPath
     *            Absolute path of the remote directory.
     * @return
     * @throws Throwable
     */
    static boolean directoryExists(SFTPv3Client client, String absDstPath) throws Throwable {
        try {
            client.stat(absDstPath);
            return true;
        } catch (Throwable t) {
            if (t instanceof SFTPException) {
                if (t.getMessage().indexOf("SSH_FX_NO_SUCH_FILE") >= 0) {
                    return false;
                }
            }
            throw t;
        }
    }

    /**
     * Make directories recursively.
     * 
     * @param client
     * @param absDstPath
     *            The absolute path of the remote destination.
     * @param mode
     *            The remote directory mode (permissions).
     * @return
     * @throws Throwable
     */
    static String mkdirs(SFTPv3Client client, String absDstPath, int mode) throws Throwable {
        if (directoryExists(client, absDstPath)) {
            return client.canonicalPath(absDstPath);
        }
        Stack<String> stack = new Stack<String>();
        stack.push(absDstPath);
        for (String parent = PathUtils.getParent(absDstPath); parent != null
                && !directoryExists(client, parent); parent = PathUtils.getParent(parent)) {
            stack.push(parent);
        }
        while (!stack.isEmpty()) {
            String dir = stack.pop();
            client.mkdir(dir, mode);
        }
        return client.canonicalPath(absDstPath);
    }

    /**
     * Put the input (file) stream to the specified remote destination.
     * 
     * @param client
     * @param in
     *            The input stream.
     * @param length
     *            The length of the input stream.
     * @param absDstPath
     *            Absolute path of remote destination.
     * @param attrs
     *            The posix file attributes for the remote file.
     * @param dirMode
     *            The remote directory mode (permissions).
     * @throws Throwable
     */
    static void put(SFTPv3Client client, InputStream in, long length, String absDstPath, SFTPv3FileAttributes attrs,
            int dirMode) throws Throwable {
        String dirPath = PathUtils.getParent(absDstPath);
        if (dirPath != null && !directoryExists(client, dirPath)) {
            mkdirs(client, dirPath, dirMode);
        }
        SFTPv3FileHandle f = client.createFile(absDstPath);
        try {
            SFTPOutputStream out = new SFTPOutputStream(f);
            try {
                StreamUtils.copy(in, length, out);
            } finally {
                out.close();
            }
        } finally {
            client.closeFile(f);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            _client.close();
            if (_verbose) {
                System.out.println("Thread " + Thread.currentThread().getId() + ": closed scp.put session.");
            }
        } finally {
            _connection.removeClosedSessions();
        }
    }

}
