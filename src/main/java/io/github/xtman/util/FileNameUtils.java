package io.github.xtman.util;

public class FileNameUtils {

    public static final String getFileExtension(String path) {
        String fileName = getFileName(path);
        int idx = fileName.lastIndexOf('.');
        if (idx != -1) {
            return fileName.substring(idx + 1);
        } else {
            return null;
        }
    }

    public static String removeFileExtension(String path) {
        String fileName = getFileName(path);
        int idx = fileName.lastIndexOf('.');
        if (idx != -1) {
            return fileName.substring(0, idx);
        } else {
            return fileName;
        }
    }

    public static String getFileName(String path) {
        return getFileName(path, false);
    }

    public static String getFileName(String path, boolean backslash) {
        int idx = path.lastIndexOf(backslash ? '\\' : '/');
        if (idx == -1) {
            return path;
        } else {
            return path.substring(idx + 1);
        }
    }

}
