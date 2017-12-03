package io.github.xtman.file.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class FileUtils {

    public static final boolean isPosix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    public static int getFilePermissions(File f) throws Throwable {
        int mode = 0;
        if (isPosix) {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(f.toPath());
            for (PosixFilePermission perm : permissions) {
                if (PosixFilePermission.OWNER_EXECUTE.equals(perm)) {
                    mode += 0100;
                } else if (PosixFilePermission.OWNER_WRITE.equals(perm)) {
                    mode += 0200;
                } else if (PosixFilePermission.OWNER_READ.equals(perm)) {
                    mode += 0400;
                } else if (PosixFilePermission.GROUP_EXECUTE.equals(perm)) {
                    mode += 0010;
                } else if (PosixFilePermission.GROUP_WRITE.equals(perm)) {
                    mode += 0020;
                } else if (PosixFilePermission.GROUP_READ.equals(perm)) {
                    mode += 0040;
                } else if (PosixFilePermission.OTHERS_EXECUTE.equals(perm)) {
                    mode += 0001;
                } else if (PosixFilePermission.OTHERS_WRITE.equals(perm)) {
                    mode += 0002;
                } else if (PosixFilePermission.OTHERS_READ.equals(perm)) {
                    mode += 0004;
                }
            }
        } else {
            if (f.canExecute()) {
                mode += 0110;
            }
            if (f.canWrite()) {
                mode += 0220;
            }
            if (f.canRead()) {
                mode += 0440;
            }
        }
        return mode;
    }

    public static long getLastAccessed(Path f) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(f, BasicFileAttributes.class);
        return attrs.lastAccessTime().toMillis();
    }

    public static long getLastAccessed(File f) throws IOException {
        return getLastAccessed(f.toPath());
    }

    public static int getMTime(File f) {
        return (int) (f.lastModified() / 1000);
    }

    public static int getMTime(Path f) {
        return getMTime(f.toFile());
    }

    public static int getATime(Path f) throws IOException {
        return (int) (getLastAccessed(f) / 1000);
    }

    public static int getATime(File f) throws IOException {
        return getATime(f.toPath());
    }

}
