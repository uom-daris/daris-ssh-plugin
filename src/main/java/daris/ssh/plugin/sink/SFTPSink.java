package daris.ssh.plugin.sink;

import java.io.InputStream;

import daris.ssh.client.SSHClient;

public class SFTPSink extends SSHSink {

    public static final String TYPE_NAME = "daris-sftp";

    public SFTPSink() throws Throwable {
        super(TYPE_NAME);
    }

    @Override
    public String description() throws Throwable {
        return "SFTP sink.";
    }

    @Override
    protected void put(SSHClient client, InputStream in, long length, String remoteFilePath) throws Throwable {
        client.sftpPut(in, length, remoteFilePath);
    }

}
