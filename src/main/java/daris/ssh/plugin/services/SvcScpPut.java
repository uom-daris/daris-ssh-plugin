package daris.ssh.plugin.services;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ScpClient;

public class SvcScpPut extends AbstractSshPutService {

    public static final String SERVICE_NAME = "daris.scp.put";

    @Override
    protected ScpClient createTransferClient(Connection cxn, String directory) throws Throwable {
        return cxn.createScpClient(directory);
    }

    @Override
    public String description() {
        return "Send specified assets to remote server via scp.";
    }

    @Override
    public String name() {
        return SERVICE_NAME;
    }

}
