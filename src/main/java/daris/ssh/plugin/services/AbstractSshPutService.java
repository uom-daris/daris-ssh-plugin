package daris.ssh.plugin.services;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        this.defn.add(new Interface.Element("namespace", StringType.DEFAULT, "Source asset namespace.", 0,
                Integer.MAX_VALUE));
        this.defn.add(new Interface.Element("where", StringType.DEFAULT, "Query to select the source assets.", 0, 1));
        this.defn.add(new Interface.Element("id", AssetType.DEFAULT, "Source asset id.", 0, Integer.MAX_VALUE));
        Interface.Element cid = new Interface.Element("cid", CiteableIdType.DEFAULT, "Citeable id of source asset.", 0,
                Integer.MAX_VALUE);
        cid.add(new Interface.Attribute("recursive", BooleanType.DEFAULT, "Includes descendants. Defaults to false.",
                0));
        this.defn.add(cid);
        this.defn.add(new Interface.Element("directory", StringType.DEFAULT, "Remote destination directory.", 0, 1));
        this.defn.add(new Interface.Element("expr", StringType.DEFAULT, "Expression to generate output path.", 0, 1));

        Interface.Element inputFile = new Interface.Element("input-file", StringType.DEFAULT,
                "File name of the service input. Must be specified if service input is given.", 0, 1);
        inputFile.add(new Interface.Attribute("unarchive", BooleanType.DEFAULT,
                "Decompress if the service input is an archive file (e.g. .zip, .aar). Defaults to false", 0));
        this.defn.add(inputFile);

        Interface.Element url = new Interface.Element("url", UrlType.DEFAULT,
                "A URL to the source file/directory to be sent by sFTP. It must be accessible by the server.", 0, 1);
        url.add(new Interface.Attribute("unarchive", BooleanType.DEFAULT,
                "Decompress if it is an archive file (e.g. .zip, .aar). Defaults to false", 0));
        this.defn.add(url);
    }

    @Override
    protected void execute(Connection cxn, Element args, Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable {
        String namespace = args.value("namespace");
        String where = args.value("where");
        Collection<String> ids = args.values("id");
        List<XmlDoc.Element> cides = args.elements("cid");
        String directory = args.value("directory");
        String expr = args.value("expr");
        String pathGenerateService = getPathGenerateService(executor());
        String inputFileName = args.value("input-file");
        boolean unarchiveInput = args.booleanValue("input-file/@unarchive", false);
        if (inputs != null && inputs.size() > 0 && inputFileName == null) {
            throw new IllegalArgumentException("Missing input-file name.");
        }
        String url = args.value("url");
        if (url!=null && !url.toLowerCase().startsWith("file:")) {
            throw new IllegalArgumentException("Unsuported url: " + url + ". Only file: is supported.");
        }
        boolean unarchiveUrl = args.booleanValue("url/@unarchive");

        TransferClient client = createTransferClient(cxn, directory);
        try {
            if (namespace != null) {
                put(executor(), "namespace>='" + namespace + "'", namespace, client, expr, pathGenerateService);
            }
            if (where != null) {
                put(executor(), where, null, client, expr, pathGenerateService);
            }
            if (ids != null) {
                StringBuilder sb = new StringBuilder();
                for (String id : ids) {
                    if (sb.length() > 0) {
                        sb.append(" or ");
                    }
                    sb.append("id=" + id);
                }
                put(executor(), sb.toString(), null, client, expr, pathGenerateService);
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
                put(executor(), sb.toString(), null, client, expr, pathGenerateService);
            }
            if (inputs != null && inputs.size() > 0) {
                PluginService.Input input = inputs.input(0);
                try {
                    put(client, inputFileName, input.stream(), input.length(), unarchiveInput);
                } finally {
                    input.stream().close();
                    input.close();
                }
            }
            if (url != null) {
                if (url.toLowerCase().startsWith("file:")) {
                    Path path = Paths.get(url.substring(5));
                    if (Files.isDirectory(path)) {
                        client.putDirectory(path, true);
                    } else {
                        InputStream fi = new BufferedInputStream(new FileInputStream(path.toFile()));
                        try {
                            put(client, path.getFileName().toString(), fi, Files.size(path), unarchiveUrl);
                        } finally {
                            fi.close();
                        }
                    }
                } else {
                    URL u = new URL(url);
                    InputStream fi = u.openStream();
                    try {
                        put(client, FileNameUtils.getFileName(u.getPath()), fi, -1, unarchiveUrl);
                    } finally {
                        fi.close();
                    }
                }
            }
        } finally {
            client.close();
        }
    }

    private static void put(TransferClient client, String fileName, InputStream in, long length, boolean unarchive)
            throws Throwable {
        System.out.println("FNAME: " +fileName);
        String ext = FileNameUtils.getFileExtension(fileName);
        if (ArchiveRegistry.isAnArchiveExtension(ext) && unarchive) {
            ArchiveInput ai = ArchiveRegistry.createInputForExtension(new SizedInputStream(in, length), ext,
                    ArchiveInput.ACCESS_RANDOM);
            String filePrefix = FileNameUtils.removeFileExtension(fileName);
            try {
                ArchiveInput.Entry e = null;
                while ((e = ai.next()) != null) {
                    String name = PathUtils.join(filePrefix, e.name());
                    try {
                        if (e.isDirectory()) {
                            client.mkdirs(name);
                        } else {
                            client.put(e.stream(), e.size(), name);
                        }
                    } finally {
                        ai.closeEntry();
                    }
                }
            } finally {
                ai.close();
            }
        } else {
            System.out.println("LENGTH: " + length);
            client.put(in, length, fileName);
        }
    }

    private static void put(ServiceExecutor executor, String where, String namespace, TransferClient client,
            String expr, String pathGenerateService) throws Throwable {

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
                    SimpleEntry<XmlDoc.Element, Output> entry = getAsset(executor, id, null);
                    XmlDoc.Element ae = entry.getKey();
                    Output output = entry.getValue();
                    client.put(output.stream(), output.length() < 0 ? ae.longValue("content/size") : output.length(),
                            path);

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
