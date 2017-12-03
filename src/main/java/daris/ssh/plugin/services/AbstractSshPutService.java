package daris.ssh.plugin.services;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;

import arc.mf.plugin.PluginTask;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.AssetType;
import arc.mf.plugin.dtype.BooleanType;
import arc.mf.plugin.dtype.CiteableIdType;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlDocMaker;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.TransferClient;
import io.github.xtman.util.PathUtils;

public abstract class AbstractSshPutService extends AbstractSshService {

    public static final int PAGE_SIZE = 10000;

    protected AbstractSshPutService() {
        this.defn.add(new Interface.Element("namespace", StringType.DEFAULT, "Source asset namespace.", 0,
                Integer.MAX_VALUE));
        this.defn.add(new Interface.Element("where", StringType.DEFAULT, "Query to select the source assets.", 0, 1));
        this.defn.add(new Interface.Element("id", AssetType.DEFAULT, "Source asset id.", 1, Integer.MAX_VALUE));
        Interface.Element cid = new Interface.Element("cid", CiteableIdType.DEFAULT, "Citeable id of source asset.", 1,
                Integer.MAX_VALUE);
        cid.add(new Interface.Attribute("recursive", BooleanType.DEFAULT, "Includes descendants. Defaults to false.",
                0));
        this.defn.add(cid);
        this.defn.add(new Interface.Element("directory", StringType.DEFAULT, "Remote destination directory.", 1, 1));
        this.defn.add(new Interface.Element("expr", StringType.DEFAULT, "Expression to generate output path.", 1, 1));
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
        } finally {
            client.close();
        }
    }

    private static void put(ServiceExecutor executor, String where, String namespace, TransferClient client,
            String expr, String pathGenerateService) throws Throwable {

        long total = -1;
        int idx = 1;
        int remaining = Integer.MAX_VALUE;
        XmlDoc.Element re = null;
        while (remaining > 0) {
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
                    PluginTask.threadTaskCompleted();
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

}
