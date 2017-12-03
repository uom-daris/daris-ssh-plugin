package daris.ssh.plugin.services;

import java.io.InputStream;
import java.util.Collection;

import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.TransferClient.GetHandler;

public abstract class AbstractSshGetService extends AbstractSshService {

    AbstractSshGetService() {
        this.defn.add(
                new Interface.Element("path", StringType.DEFAULT, "remote source file path.", 1, Integer.MAX_VALUE));
        this.defn.add(new Interface.Element("namespace", StringType.DEFAULT, "destination namespace.", 1, 1));
    }

    @Override
    public Access access() {
        return ACCESS_MODIFY;
    }

    @Override
    protected void execute(Connection cxn, Element args, Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable {
        Collection<String> paths = args.values("path");
        final String namespace = args.value("namespace");
        GetHandler gh = new GetHandler() {

            @Override
            public void getFile(FileAttrs file, InputStream in) throws Throwable {
                createOrUpdateAsset(executor(), file, in, namespace);
            }

            @Override
            public void getDirectory(FileAttrs dir) throws Throwable {
                createNamespace(executor(), dir, namespace);

            }
        };
        execute(cxn, paths, namespace, gh, args, inputs, outputs, w);
    }

    protected abstract void execute(Connection cxn, Collection<String> paths, String namespace, GetHandler handler,
            XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable;

    static void createOrUpdateAsset(ServiceExecutor executor, FileAttrs file, InputStream in, String namespace)
            throws Throwable {
        // TODO
    }

    static void createNamespace(ServiceExecutor executor, FileAttrs dir, String namespace) throws Throwable {
        // TODO
    }
}
