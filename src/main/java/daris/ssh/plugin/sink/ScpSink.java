package daris.ssh.plugin.sink;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ConnectionBuilder;
import io.github.xtman.ssh.client.ScpClient;

public class ScpSink extends SshSink {

    public static final String TYPE_NAME = "daris-scp";

    public ScpSink() throws Throwable {
        super(TYPE_NAME);
    }

    @Override
    public String description() throws Throwable {
        return "DaRIS scp sink.";
    }

    @Override
    protected ScpClient createClient(ConnectionBuilder cb, String directory, int dirMode, int fileMode)
            throws Throwable {
        Connection cxn = cb.build();
        return cxn.createScpClient(directory, "UTF-8", dirMode, fileMode, false, false);
    }

}
