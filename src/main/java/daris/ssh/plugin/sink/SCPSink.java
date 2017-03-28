package daris.ssh.plugin.sink;

import java.io.InputStream;

import daris.ssh.client.SSHClient;

public class SCPSink extends SSHSink {

    public static final String TYPE_NAME = "daris-scp";

    public SCPSink() throws Throwable {
        super(TYPE_NAME);
    }

    @Override
    public String description() throws Throwable {
        return "DaRIS SCP sink.";
    }

    @Override
    protected void put(SSHClient client, InputStream in, long length, String remoteFilePath) throws Throwable {
        client.scpPut(in, length, remoteFilePath);
    }

}
