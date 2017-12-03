package io.github.xtman.ssh.client;

import io.github.xtman.util.PathUtils;

public class FileAttrs {

    private String _path;
    private Integer _mode;
    private long _length;
    private Integer _mtime;
    private Integer _atime;
    private boolean _isDirectory;

    /**
     * Directory attributes.
     * 
     * @param path
     * @param mode
     * @param mtime
     * @param atime
     */
    public FileAttrs(String path, Integer mode, Integer mtime, Integer atime) {
        this(path, mode, 0L, mtime, atime, true);
    }

    /**
     * Regular file attributes.
     * 
     * @param path
     * @param mode
     * @param length
     * @param mtime
     * @param atime
     */
    public FileAttrs(String path, Integer mode, long length, Integer mtime, Integer atime) {
        this(path, mode, length, mtime, atime, false);
    }

    protected FileAttrs(String path, Integer mode, long length, Integer mtime, Integer atime, boolean isDirectory) {
        _path = path;
        _mode = mode;
        _length = length;
        _mtime = mtime;
        _atime = atime;
        _isDirectory = isDirectory;
    }

    public String name() {
        return PathUtils.getLastComponent(_path);
    }

    public String path() {
        return _path;
    }

    public Integer mode() {
        return _mode;
    }

    public long length() {
        return _length;
    }

    public Integer mtime() {
        return _mtime;
    }

    public void setMTime(Integer mtime) {
        _mtime = mtime;
    }

    public Integer atime() {
        return _atime;
    }

    public void setATime(Integer atime) {
        _atime = atime;
    }

    public boolean isDirectory() {
        return _isDirectory;
    }

    public boolean isRegularFile() {
        return !isDirectory();
    }

}
