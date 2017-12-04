package daris.ssh.plugin.services;

import java.io.InputStream;
import java.util.Collection;

import arc.mf.plugin.PluginService;
import arc.mf.plugin.PluginTask;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlDocMaker;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.TransferClient.GetHandler;
import io.github.xtman.util.PathUtils;

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
        if (!assetNamespaceExists(executor(), namespace)) {
            throw new IllegalArgumentException("Asset namespace: '" + namespace + "' does not exist.");
        }
        GetHandler gh = new GetHandler() {

            @Override
            public void getFile(FileAttrs file, InputStream in) throws Throwable {
                PluginTask.checkIfThreadTaskAborted();
                createOrUpdateAsset(executor(), file, in, namespace);
                PluginTask.threadTaskCompleted(1);
                PluginTask.checkIfThreadTaskAborted();
            }

            @Override
            public void getDirectory(FileAttrs dir) throws Throwable {
                PluginTask.checkIfThreadTaskAborted();
                createAssetNamespace(executor(), dir, namespace);
                PluginTask.checkIfThreadTaskAborted();
            }
        };
        execute(cxn, paths, namespace, gh, args, inputs, outputs, w);
    }

    protected abstract void execute(Connection cxn, Collection<String> paths, String namespace, GetHandler handler,
            XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable;

    static void createOrUpdateAsset(ServiceExecutor executor, FileAttrs file, InputStream in, String namespace)
            throws Throwable {
        String path = PathUtils.join(namespace, file.path());
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("id", "path=" + path);
        dm.add("create", true);
        dm.add("name", file.name());
        dm.push("meta");
        dm.push("mf-name");
        dm.add("name", file.name());
        dm.pop();
        dm.pop();
        PluginService.Input input = new PluginService.Input(in, file.length(), null, file.path());
        executor.execute("asset.set", dm.root(), new PluginService.Inputs(input), null);
    }

    static void createAssetNamespace(ServiceExecutor executor, FileAttrs dir, String namespace) throws Throwable {
        String ns = PathUtils.join(namespace, dir.path());
        if (!assetNamespaceExists(executor, ns)) {
            XmlDocMaker dm = new XmlDocMaker("args");
            dm.add("namespace", new String[] { "all", "true" }, ns);
            executor.execute("asset.namespace.create", dm.root());
        }
    }

    static boolean assetNamespaceExists(ServiceExecutor executor, String namespace) throws Throwable {
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("namespace", namespace);
        return executor.execute("asset.namespace.exists", dm.root()).booleanValue("exists");
    }
}
