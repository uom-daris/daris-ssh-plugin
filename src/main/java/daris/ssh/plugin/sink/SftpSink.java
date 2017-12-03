package daris.ssh.plugin.sink;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ConnectionBuilder;
import io.github.xtman.ssh.client.SftpClient;

public class SftpSink extends SshSink {

    public static final String TYPE_NAME = "daris-sftp";

    public SftpSink() throws Throwable {
        super(TYPE_NAME);
    }

    @Override
    public String description() throws Throwable {
        return "SFTP sink.";
    }

    @Override
    protected SftpClient createClient(ConnectionBuilder cb, String directory, int dirMode, int fileMode)
            throws Throwable {
        Connection cxn = cb.build();
        return cxn.createSftpClient(directory, "UTF-8", dirMode, fileMode, false, false);
    }

}
