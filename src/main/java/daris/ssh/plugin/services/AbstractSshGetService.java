package daris.ssh.plugin.services;

import java.io.InputStream;
import java.util.Collection;

import arc.mf.plugin.PluginService;
import arc.mf.plugin.PluginTask;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.StringType;
import arc.mf.plugin.dtype.XmlDocType;
import arc.utils.Task.ExAborted;
import arc.xml.XmlDoc;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlDocMaker;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.TransferClient;
import io.github.xtman.ssh.client.TransferClient.GetHandler;
import io.github.xtman.util.PathUtils;

public abstract class AbstractSshGetService extends AbstractSshService {

    AbstractSshGetService() {
        this.defn.add(
                new Interface.Element("path", StringType.DEFAULT, "remote source file path.", 1, Integer.MAX_VALUE));
        this.defn.add(new Interface.Element("namespace", StringType.DEFAULT, "destination namespace.", 1, 1));

        Interface.Element readOnly = new Interface.Element("read-only", XmlDocType.DEFAULT,
                "If set to true, the asset cannot be modified once created. Defaults to false.", 0, 1);
        this.defn.add(readOnly);

        Interface.Element worm = new Interface.Element("worm", XmlDocType.DEFAULT,
                "Explicitly sets the WORM (write once read multiple) state for the assets. The state may be 'upgraded' (stronger and longer) but never diminished. NOTE: all sub-elements must follow the instructions of service 'asset.worm.set'",
                0, 1);
        worm.setIgnoreDescendants(true);
        this.defn.add(worm);

    }

    @Override
    public Access access() {
        return ACCESS_MODIFY;
    }

    @Override
    protected void execute(Connection cxn, Element args, Inputs inputs, Outputs outputs, XmlWriter w, OnError onError)
            throws Throwable {
        Collection<String> paths = args.values("path");
        final String namespace = args.value("namespace");
        if (!assetNamespaceExists(executor(), namespace)) {
            throw new IllegalArgumentException("Asset namespace: '" + namespace + "' does not exist.");
        }
        final boolean readOnly = args.booleanValue("read-only");
        final XmlDoc.Element worm = args.element("worm");
        GetHandler gh = new GetHandler() {

            @Override
            public void getFile(FileAttrs file, InputStream in) throws Throwable {
                PluginTask.checkIfThreadTaskAborted();
                PluginTask.setCurrentThreadActivity("getting remote file: " + file.path());
                createOrUpdateAsset(executor(), file, in, namespace, readOnly, worm);
                PluginTask.clearCurrentThreadActivity();
                PluginTask.threadTaskCompleted(1);
                PluginTask.checkIfThreadTaskAborted();
            }

            @Override
            public void getDirectory(FileAttrs dir) throws Throwable {
                PluginTask.checkIfThreadTaskAborted();
                PluginTask.setCurrentThreadActivity("getting remote directory: " + dir.path());
                createAssetNamespace(executor(), dir, namespace);
                PluginTask.clearCurrentThreadActivity();
                PluginTask.checkIfThreadTaskAborted();
            }
        };

        execute(cxn, paths, namespace, gh, args, inputs, outputs, w, onError);
    }

    protected void get(TransferClient client, String path, GetHandler gh, int retry, boolean stopOnError, XmlWriter w)
            throws Throwable {
        try {
            client.get(path, gh);
        } catch (Throwable e) {
            if (e instanceof ExAborted || retry <= 0) {
                if (stopOnError) {
                    throw e;
                } else {
                    w.add("failed",
                            new String[] { "error", e.getClass().getSimpleName() + ":" + e.getMessage(), path });
                    // continue
                }
            } else {
                retry--;
                get(client, path, gh, retry, stopOnError, w);
            }
        }
    }

    protected abstract void execute(Connection cxn, Collection<String> paths, String namespace, GetHandler handler,
            XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w, OnError onError) throws Throwable;

    static void createOrUpdateAsset(ServiceExecutor executor, FileAttrs file, InputStream in, String namespace,
            boolean readOnly, XmlDoc.Element worm) throws Throwable {
        String path = PathUtils.join(namespace, file.path());
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.push("service", new String[] { "name", "asset.set" });
        dm.add("id", "path=" + path);
        dm.add("create", true);
        dm.add("name", file.name());
        dm.push("meta");
        dm.push("mf-name");
        dm.add("name", file.name());
        dm.pop();
        dm.pop();
        dm.pop();
        if (readOnly) {
            dm.push("service", new String[] { "asset.set.readonly" });
            dm.add("readonly", readOnly);
            dm.add("id", "path=" + path);
            dm.pop();
        }
        if (worm != null) {
            dm.push("service", new String[] { "asset.worm.set" });
            dm.add(worm, false);
            dm.add("id", "path=" + path);
            dm.pop();
        }
        PluginService.Input input = new PluginService.Input(in, file.length(), null, file.path());
        try {
            executor.execute("service.execute", dm.root(), new PluginService.Inputs(input), null);
        } finally {
            input.close();
        }
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
