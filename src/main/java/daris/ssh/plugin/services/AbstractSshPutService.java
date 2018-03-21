package daris.ssh.plugin.services;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;

import arc.archive.ArchiveInput;
import arc.archive.ArchiveRegistry;
import arc.mf.plugin.PluginService;
import arc.mf.plugin.PluginTask;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.AssetType;
import arc.mf.plugin.dtype.BooleanType;
import arc.mf.plugin.dtype.CiteableIdType;
import arc.mf.plugin.dtype.StringType;
import arc.mf.plugin.dtype.UrlType;
import arc.streams.SizedInputStream;
import arc.utils.Task.ExAborted;
import arc.xml.XmlDoc;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlDocMaker;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.TransferClient;
import io.github.xtman.util.FileNameUtils;
import io.github.xtman.util.PathUtils;

public abstract class AbstractSshPutService extends AbstractSshService {

    public static final int PAGE_SIZE = 10000;

    protected AbstractSshPutService() {
        /*
         * src: namespace
         */
        this.defn.add(new Interface.Element("namespace", StringType.DEFAULT, "Source asset namespace.", 0,
                Integer.MAX_VALUE));

        /*
         * src: query
         */
        this.defn.add(new Interface.Element("where", StringType.DEFAULT, "Query to select the source assets.", 0, 1));

        /*
         * src: id
         */
        this.defn.add(new Interface.Element("id", AssetType.DEFAULT, "Source asset id.", 0, Integer.MAX_VALUE));

        /*
         * src: cid
         */
        Interface.Element cid = new Interface.Element("cid", CiteableIdType.DEFAULT, "Citeable id of source asset.", 0,
                Integer.MAX_VALUE);
        cid.add(new Interface.Attribute("recursive", BooleanType.DEFAULT, "Includes descendants. Defaults to false.",
                0));
        this.defn.add(cid);

        /*
         * src: input
         */
        this.defn.add(new Interface.Element("input-file", StringType.DEFAULT,
                "File name of the service input. Must be specified if service input is given.", 0, 1));

        /*
         * src: url
         */
        this.defn.add(new Interface.Element("url", UrlType.DEFAULT,
                "A URL to the source file/directory to be sent by sFTP. It must be accessible by the server.", 0, 1));

        /*
         * dst: directory
         */
        this.defn.add(new Interface.Element("directory", StringType.DEFAULT, "Remote destination directory.", 0, 1));

        /*
         * unarchive .zip/.aar files?
         */
        this.defn.add(new Interface.Element("unarchive", BooleanType.DEFAULT,
                "Decompress if it is an archive file (e.g. .zip, .aar). Defaults to false", 0, 1));

        /*
         * path expression: only for assets
         */
        this.defn.add(new Interface.Element("expr", StringType.DEFAULT, "Expression to generate output path.", 0, 1));

    }

    @Override
    protected void execute(Connection cxn, Element args, Inputs inputs, Outputs outputs, XmlWriter w, OnError onError)
            throws Throwable {
        String namespace = args.value("namespace");
        String where = args.value("where");
        Collection<String> ids = args.values("id");
        List<XmlDoc.Element> cides = args.elements("cid");
        String directory = args.value("directory");
        String expr = args.value("expr");
        String pathGenerateService = getPathGenerateService(executor());
        String inputFileName = args.value("input-file");
        if (inputs != null && inputs.size() > 0 && inputFileName == null) {
            throw new IllegalArgumentException("Missing input-file name.");
        }
        URI url = args.elementExists("url") ? URI.create(args.value("url")) : null;
        // if (url != null && !"file".equals(url.getScheme())) {
        // throw new IllegalArgumentException("Only file url is supported.");
        // }

        boolean unarchive = args.booleanValue("unarchive", false);

        TransferClient client = createTransferClient(cxn, directory);
        try {
            if (namespace != null) {
                put(executor(), "namespace>='" + namespace + "'", namespace, client, expr, pathGenerateService,
                        unarchive, onError, w);
            }
            if (where != null) {
                put(executor(), where, null, client, expr, pathGenerateService, unarchive, onError, w);
            }
            if (ids != null) {
                StringBuilder sb = new StringBuilder();
                for (String id : ids) {
                    if (sb.length() > 0) {
                        sb.append(" or ");
                    }
                    sb.append("id=" + id);
                }
                put(executor(), sb.toString(), null, client, expr, pathGenerateService, unarchive, onError, w);
            }
            if (cides != null) {
                StringBuilder sb = new StringBuilder();
                for (XmlDoc.Element cide : cides) {
                    String cid = cide.value();
                    boolean recursive = cide.booleanValue("@recursive", false);
                    if (sb.length() > 0) {
                        sb.append(" or ");
                    }
                    if (recursive) {
                        sb.append("(");
                    }
                    sb.append("cid='").append(cid).append("'");
                    if (recursive) {
                        sb.append(" or cid starts with '").append(cid).append("')");
                    }
                }
                put(executor(), sb.toString(), null, client, expr, pathGenerateService, unarchive, onError, w);
            }
            if (inputs != null && inputs.size() > 0) {
                try {
                    PluginService.Input input = inputs.input(0);
                    try {
                        put(client, inputFileName, input.stream(), input.length(), unarchive);
                    } finally {
                        input.stream().close();
                        input.close();
                    }
                } catch (Throwable e) {
                    w.add("failed",
                            new String[] { "error", e.getMessage(), "input", inputFileName, "dst", inputFileName });
                    if (e instanceof ExAborted || e instanceof InterruptedException || onError.stopOnError()) {
                        throw e;
                    }
                    // input stream may already be consumed. Cannot retry.
                }
            }
            if (url != null) {
                try {
                    if ("file".equalsIgnoreCase(url.getScheme())) {
                        File f = new File(url);
                        if (f.isDirectory()) {
                            try {
                                PluginTask.setCurrentThreadActivity("putting directory: " + f.getName());
                                client.putDirectory(f.toPath(), true);
                            } finally {
                                PluginTask.clearCurrentThreadActivity();
                            }
                        } else {
                            InputStream fi = new BufferedInputStream(new FileInputStream(f));
                            try {
                                put(client, f.getName(), fi, f.length(), unarchive);
                            } finally {
                                fi.close();
                            }
                        }
                    } else {
                        InputStream fi = url.toURL().openStream();
                        try {
                            put(client, FileNameUtils.getFileName(url.getPath()), fi, -1, unarchive);
                        } finally {
                            fi.close();
                        }
                    }
                } catch (Throwable e) {
                    w.add("failed", new String[] { "error", e.getMessage(), "url", url.toString(), "dst",
                            FileNameUtils.getFileName(url.getPath()) });
                    if (e instanceof ExAborted || e instanceof InterruptedException || onError.stopOnError()) {
                        throw e;
                    }
                    // url may already be consumed. Cannot retry.
                }
            }
        } finally {
            client.close();
        }
    }

    private static void put(TransferClient client, String dstPath, InputStream in, long length, boolean unarchive)
            throws Throwable {
        PluginTask.checkIfThreadTaskAborted();
        // System.out.println("FNAME: " + fileName);
        String ext = FileNameUtils.getFileExtension(dstPath);
        if (ArchiveRegistry.isAnArchiveExtension(ext) && unarchive) {
            ArchiveInput ai = ArchiveRegistry.createInputForExtension(new SizedInputStream(in, length), ext,
                    ArchiveInput.ACCESS_RANDOM);
            String filePrefix = FileNameUtils.removeFileExtension(dstPath);
            try {
                ArchiveInput.Entry e = null;
                while ((e = ai.next()) != null) {
                    String name = PathUtils.join(filePrefix, e.name());
                    try {
                        PluginTask.setCurrentThreadActivity("putting file: " + name);
                        if (e.isDirectory()) {
                            client.mkdirs(name);
                        } else {
                            client.put(e.stream(), e.size(), name);
                        }
                    } finally {
                        ai.closeEntry();
                        PluginTask.clearCurrentThreadActivity();
                    }
                }
            } finally {
                ai.close();
            }
        } else {
            // System.out.println("LENGTH: " + length);
            try {
                PluginTask.setCurrentThreadActivity("putting file: " + dstPath);
                client.put(in, length, dstPath);
            } finally {
                PluginTask.clearCurrentThreadActivity();
            }
        }
    }

    private static void put(ServiceExecutor executor, String where, String namespace, TransferClient client,
            String expr, String pathGenerateService, boolean unarchive, OnError onError, XmlWriter w) throws Throwable {

        long total = -1;
        int idx = 1;
        int remaining = Integer.MAX_VALUE;
        XmlDoc.Element re = null;
        while (remaining > 0) {

            PluginTask.checkIfThreadTaskAborted();

            XmlDocMaker dm = new XmlDocMaker("args");
            dm.add("where", where);
            dm.add("count", true);
            dm.add("idx", idx);
            dm.add("size", PAGE_SIZE);
            if (expr != null) {
                dm.add("action", "pipe");
                dm.push("service", new String[] { "name", pathGenerateService });
                dm.add("expr", expr);
                dm.pop();
                dm.add("pipe-generate-result-xml", true);
            } else {
                dm.add("action", "get-path");
            }
            re = executor.execute("asset.query", dm.root());
            if (total < 0) {
                total = re.longValue("cursor/total", 0);
                if (total > 0) {
                    PluginTask.threadTaskBeginSetOf(total);
                }
            }
            remaining = re.intValue("cursor/remaining", 0);
            List<XmlDoc.Element> pes = re.elements("path");
            if (pes != null) {
                for (XmlDoc.Element pe : pes) {

                    PluginTask.checkIfThreadTaskAborted();

                    String id = pe.value("@id");
                    String path = pe.value();
                    if (namespace != null) {
                        path = PathUtils.getRelativePath(PathUtils.trimSlash(path), PathUtils.trimSlash(namespace));
                    } else {
                        path = PathUtils.trimSlash(path);
                    }
                    try {
                        putAsset(executor, id, path, unarchive, client, onError.retry());
                    } catch (Throwable e) {
                        w.add("failed", new String[] { "asset", id, "dst", path });
                        if (onError.stopOnError() || e instanceof ExAborted || e instanceof InterruptedException) {
                            throw e;
                        }
                        // continue without break
                    }
                    PluginTask.checkIfThreadTaskAborted();
                    PluginTask.threadTaskCompleted(1);
                }
            }
            idx += PAGE_SIZE;
        }
        if (total > 0) {
            PluginTask.threadTaskEndSet();
        }
    }

    private static void putAsset(ServiceExecutor executor, String assetId, String dstPath, boolean unarchive,
            TransferClient client, int retry) throws Throwable {
        try {
            SimpleEntry<XmlDoc.Element, Output> entry = getAsset(executor, assetId, null);
            XmlDoc.Element ae = entry.getKey();
            Output output = entry.getValue();
            try {
                put(client, dstPath, output.stream(),
                        output.length() < 0 ? ae.longValue("content/size") : output.length(), unarchive);
            } finally {
                output.stream().close();
                output.close();
            }
        } catch (Throwable e) {
            if (e instanceof IOException && retry > 0) {
                // retry if set
                putAsset(executor, assetId, dstPath, unarchive, client, retry--);
            } else {
                throw e;
            }
        }
    }

    private static SimpleEntry<XmlDoc.Element, Output> getAsset(ServiceExecutor executor, String id, String cid)
            throws Throwable {
        XmlDocMaker dm = new XmlDocMaker("args");
        if (id != null) {
            dm.add("id", id);
        } else {
            dm.add("cid", cid);
        }
        Outputs outputs = new Outputs(1);
        XmlDoc.Element re = executor.execute("asset.get", dm.root(), null, outputs);
        return new SimpleEntry<XmlDoc.Element, Output>(re.element("asset"), outputs.output(0));
    }

    private static String getPathGenerateService(ServiceExecutor executor) throws Throwable {
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("service", "daris.asset.path.generate");
        boolean exists = executor.execute("system.service.exists", dm.root()).booleanValue("exists");
        if (exists) {
            return "daris.asset.path.generate";
        } else {
            return "asset.path.generate";
        }
    }

    protected abstract TransferClient createTransferClient(Connection cxn, String directory) throws Throwable;

    @Override
    public Access access() {
        return ACCESS_ACCESS;
    }

    public int maxNumberOfInputs() {
        return 1;
    }

    public int minNumberOfInputs() {
        return 0;
    }

}
