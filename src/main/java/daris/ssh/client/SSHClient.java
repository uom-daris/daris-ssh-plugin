package daris.ssh.client;

import java.io.Closeable;
import java.io.InputStream;

public interface SSHClient extends Closeable {

    void scpPut(InputStream in, long length, String remoteFilePath) throws Throwable;

    void sftpPut(InputStream in, long length, String remoteFilePath) throws Throwable;

    void mkdirs(String remoteDirPath) throws Throwable;
}
