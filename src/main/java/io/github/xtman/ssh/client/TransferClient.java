package io.github.xtman.ssh.client;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

public interface TransferClient extends Channel {

    public static final int DEFAULT_DIRECTORY_MODE = 0755;

    public static final int DEFAULT_FILE_MODE = 0644;

    public static interface GetHandler {

        void getFile(FileAttrs file, InputStream in) throws Throwable;

        void getDirectory(FileAttrs dir) throws Throwable;

    }

    void get(String remotePath, GetHandler h) throws Throwable;

    void get(String remotePath, File dstDir) throws Throwable;

    void getFile(String remotePath, OutputStream out) throws Throwable;

    void getFile(String remotePath, File dstDir) throws Throwable;

    void getDirectory(String remotePath, File dstDir) throws Throwable;

    void put(FileAttrs file, InputStream in) throws Throwable;

    void put(InputStream in, long length, Integer mode, Integer mtime, Integer atime, String dstPath) throws Throwable;

    void put(InputStream in, long length, String dstPath) throws Throwable;

    void put(File f, String dstPath) throws Throwable;

    void put(Path f, String dstPath) throws Throwable;

    void putDirectory(File dir) throws Throwable;

    void putDirectory(File dir, boolean self) throws Throwable;

    void putDirectory(Path dir) throws Throwable;

    void putDirectory(Path dir, boolean self) throws Throwable;

    void mkdirs(FileAttrs dir) throws Throwable;

    void mkdirs(String dir) throws Throwable;

    boolean compress();

    void setCompress(boolean compress);

    boolean preserve();

    void setPreserve(boolean preserve);

    Integer defaultDirectoryMode();

    void setDefaultDirectoryMode(Integer dirMode);

    Integer defaultFileMode();

    void setDefaultFileMode(Integer fileMode);

}
