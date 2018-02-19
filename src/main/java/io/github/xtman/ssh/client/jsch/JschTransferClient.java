package io.github.xtman.ssh.client.jsch;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import io.github.xtman.file.util.FileUtils;
import io.github.xtman.io.util.StreamUtils;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.TransferClient;
import io.github.xtman.util.PathUtils;

public abstract class JschTransferClient<T extends com.jcraft.jsch.Channel> implements TransferClient {

    private JschConnection _cxn;

    protected T channel;

    private String _remoteBaseDir;

    private String _encoding;

    private Integer _dirMode;

    private Integer _fileMode;

    private boolean _compress;

    private boolean _preserve;

    protected boolean verbose;

    protected JschTransferClient(JschConnection connection, T channel, String remoteBaseDir, String encoding,
            Integer dirMode, Integer fileMode, boolean compress, boolean preserve, boolean verbose) {
        _cxn = connection;

        this.channel = channel;

        _remoteBaseDir = (remoteBaseDir == null || remoteBaseDir.trim().isEmpty())
                ? Connection.DEFAULT_REMOTE_BASE_DIRECTORY
                : remoteBaseDir.trim();

        _encoding = encoding == null ? Connection.DEFAULT_ENCODING : encoding;

        _dirMode = dirMode == null ? TransferClient.DEFAULT_DIRECTORY_MODE : dirMode;

        _fileMode = fileMode == null ? TransferClient.DEFAULT_FILE_MODE : fileMode;

        this.verbose = verbose;

    }

    @Override
    public Connection connection() {
        return _cxn;
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.verbose) {
                System.out.print("closing " + channelType() + " channel ... ");
            }
            this.channel.disconnect();
            if (this.verbose) {
                System.out.println("done");
            }
        } finally {
            _cxn.removeChannel(this.channel);
        }
    }

    public String remoteBaseDirectory() {
        return _remoteBaseDir;
    }

    public void setRemoteBaseDirectory(String baseDir) throws Throwable {
        if (baseDir == null || baseDir.trim().isEmpty()) {
            _remoteBaseDir = Connection.DEFAULT_REMOTE_BASE_DIRECTORY;
        } else {
            _remoteBaseDir = baseDir.trim();
        }
    }

    @Override
    public String encoding() {
        return _encoding;
    }

    @Override
    public void setEncoding(String encoding) {
        _encoding = encoding;
    }

    @Override
    public boolean verbose() {
        return this.verbose;
    }

    @Override
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public Integer defaultDirectoryMode() {
        return _dirMode;
    }

    @Override
    public void setDefaultDirectoryMode(Integer dirMode) {
        _dirMode = dirMode;
    }

    @Override
    public Integer defaultFileMode() {
        return _fileMode;
    }

    @Override
    public void setDefaultFileMode(Integer fileMode) {
        _fileMode = fileMode;
    }

    @Override
    public boolean compress() {
        return _compress;
    }

    @Override
    public void setCompress(boolean compress) {
        _compress = compress;
    }

    @Override
    public boolean preserve() {
        return _preserve;
    }

    @Override
    public void setPreserve(boolean preserve) {
        _preserve = preserve;
    }

    @Override
    public void get(String remotePath, final File dstDir) throws Throwable {
        get(remotePath, new GetHandler() {

            @Override
            public void getFile(FileAttrs fileAttrs, InputStream in) throws Throwable {

                File of = new File(PathUtils.join(dstDir.getAbsolutePath(), fileAttrs.path()));
                File dir = of.getParentFile();
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                OutputStream os = new BufferedOutputStream(new FileOutputStream(of));
                try {
                    StreamUtils.copy(in, os);
                } finally {
                    os.close();
                }
                // TODO set attrs
            }

            @Override
            public void getDirectory(FileAttrs dirAttrs) throws Throwable {
                File dir = new File(PathUtils.join(dstDir.getAbsolutePath(), dirAttrs.path()));
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                // TODO set attrs
            }
        });
    }

    @Override
    public void getDirectory(String remotePath, File dstDir) throws Throwable {
        get(remotePath, dstDir);
    }

    @Override
    public void getFile(final String remotePath, final OutputStream out) throws Throwable {
        get(remotePath, new GetHandler() {

            @Override
            public void getFile(FileAttrs file, InputStream in) throws Throwable {
                StreamUtils.copy(in, out);
            }

            @Override
            public void getDirectory(FileAttrs dir) throws Throwable {
                throw new IOException("Remote directory: '" + remotePath + "' is not a regular file.");
            }
        });
    }

    @Override
    public void getFile(String remotePath, File dstDir) throws Throwable {
        File of = new File(dstDir, PathUtils.getLastComponent(remotePath));
        OutputStream os = new BufferedOutputStream(new FileOutputStream(of));
        try {
            getFile(remotePath, os);
        } finally {
            os.close();
        }
    }

    @Override
    public void mkdirs(FileAttrs dir) throws Throwable {
        assert dir.isDirectory();
        put(dir, null);
    }

    @Override
    public void mkdirs(String dir) throws Throwable {
        mkdirs(new FileAttrs(dir, this.defaultDirectoryMode(), null, null));
    }

    @Override
    public void put(InputStream in, long length, Integer mode, Integer mtime, Integer atime, String dstPath)
            throws Throwable {
        put(new FileAttrs(dstPath, mode, length, mtime, atime), in);
    }

    @Override
    public void put(InputStream in, long length, String dstPath) throws Throwable {
        put(in, length, this.defaultFileMode(), null, null, dstPath);
    }

    @Override
    public void put(File f, String dstPath) throws Throwable {
        InputStream in = new BufferedInputStream(new FileInputStream(f));
        try {
            int mode = FileUtils.getFilePermissions(f);
            int mtime = FileUtils.getMTime(f);
            int atime = FileUtils.getATime(f);
            put(in, f.length(), mode, mtime, atime, dstPath);
        } finally {
            in.close();
        }
    }

    @Override
    public void put(Path f, String dstPath) throws Throwable {
        put(f.toFile(), dstPath);
    }

    @Override
    public void putDirectory(File dir) throws Throwable {
        putDirectory(dir, true);
    }

    @Override
    public void putDirectory(File dir, boolean self) throws Throwable {
        putDirectory(dir.toPath(), self);
    }

    @Override
    public void putDirectory(final Path dir) throws Throwable {
        putDirectory(dir, true);
    }

    @Override
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
}
