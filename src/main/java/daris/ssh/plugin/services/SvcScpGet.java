package daris.ssh.plugin.services;

import java.util.Collection;

import arc.xml.XmlDoc.Element;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ScpClient;
import io.github.xtman.ssh.client.TransferClient.GetHandler;
import io.github.xtman.util.PathUtils;

public class SvcScpGet extends AbstractSshGetService {

    public static final String SERVICE_NAME = "daris.scp.get";

    public SvcScpGet() {

    }

    @Override
    public String description() {
        return "Secure copy files from remote SSH server to the specified namespace.";
    }

    @Override
    public String name() {
        return SERVICE_NAME;
    }

    @Override
    protected void execute(Connection cxn, Collection<String> paths, String namespace, GetHandler gh, Element args,
            Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable {
        for (String path : paths) {
            String parent = PathUtils.getParent(path);
            String name = PathUtils.getLastComponent(path);
            ScpClient scp = cxn.createScpClient(parent);
            try {
                scp.get(parent == null ? path : name, gh);
            } finally {
                scp.close();
            }
        }
    }

}
