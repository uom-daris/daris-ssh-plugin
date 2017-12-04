package daris.plugin.sink.util;

import arc.mf.plugin.ServiceExecutor;
import arc.xml.XmlDoc;
import arc.xml.XmlDocMaker;
import io.github.xtman.util.PathUtils;

public class OutputPath {

    public static String generateOutputPath(ServiceExecutor executor, String assetSpecificOutputPath,
            String contextualOutputPath, String layoutPattern, XmlDoc.Element assetMeta, boolean unarchive)
            throws Throwable {
        return generateOutputPath(null, executor, assetSpecificOutputPath, contextualOutputPath, layoutPattern,
                assetMeta, unarchive);
    }

    public static String generateOutputPath(String baseDir, ServiceExecutor executor, String assetSpecificOutputPath,
            String contextualOutputPath, String layoutPattern, XmlDoc.Element assetMeta, boolean unarchive)
            throws Throwable {

        String ext = assetMeta == null ? null : assetMeta.value("content/type/@ext");
        String assetId = assetMeta == null ? null : assetMeta.value("@id");
        String assetPath = assetMeta == null ? null : assetMeta.value("path");
        String assetName = assetMeta == null ? null : assetMeta.value("name");
        String assetNamespace = assetMeta == null ? null : assetMeta.value("namespace");

        String outputPath = null;
        if (assetSpecificOutputPath != null) {
            outputPath = assetSpecificOutputPath;
        } else if (assetId != null && layoutPattern != null && executor != null) {
            outputPath = generateAssetPath(executor, assetId, layoutPattern);
        } else if (contextualOutputPath != null) {
            // contextualOutputPath is usually asset.namespace if called by
            // asset.get
            outputPath = PathUtils.trimSlash(contextualOutputPath);
            if (assetNamespace != null) {
                if (outputPath.equals(PathUtils.trimSlash(assetNamespace))) {
                    if (assetName != null) {
                        outputPath = PathUtils.join(outputPath, assetName);
                    } else {
                        outputPath = PathUtils.join(outputPath, "__asset_id__" + assetId);
                    }
                }
            }
        } else if (assetPath != null) {
            outputPath = assetPath;
        } else {
            StringBuilder sb = new StringBuilder();
            if (assetNamespace != null) {
                sb.append(PathUtils.trimLeadingSlash(assetNamespace));
                sb.append("/");
            }
            if (assetName != null) {
                sb.append(assetName.replace('/', '_'));
            } else {
                sb.append("__asset_id__").append(assetId);
            }
            outputPath = sb.toString();
        }
        outputPath = outputPath == null ? null : PathUtils.trimLeadingSlash(outputPath.trim());
        if (outputPath == null || outputPath.isEmpty()) {
            throw new Exception("Failed to generate output path.");
        }
        if (unarchive) {
            if (ext != null && (outputPath.endsWith("." + ext) || outputPath.endsWith("." + ext.toUpperCase()))) {
                outputPath = outputPath.substring(0, outputPath.length() - ext.length() - 1);
            }
        } else {
            if (ext != null && !(outputPath.endsWith("." + ext) || outputPath.endsWith("." + ext.toUpperCase()))) {
                outputPath = outputPath + "." + ext;
            }
        }
        if (baseDir == null) {
            return outputPath;
        } else {
            return PathUtils.join(baseDir.trim(), outputPath);
        }
    }

    public static final String SERVICE_ASSET_PATH_GENERATE = "asset.path.generate";
    public static final String SERVICE_DARIS_ASSET_PATH_GENERATE = "daris.asset.path.generate";

    public static final String getAssetPathGenerateService(ServiceExecutor executor) throws Throwable {
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("service", SERVICE_DARIS_ASSET_PATH_GENERATE);
        boolean exists = executor.execute("system.service.exists", dm.root()).booleanValue("exists");
        if (exists) {
            return SERVICE_DARIS_ASSET_PATH_GENERATE;
        } else {
            return SERVICE_ASSET_PATH_GENERATE;
        }
    }

    public static final String generateAssetPath(ServiceExecutor executor, String id, String expr) throws Throwable {
        String service = getAssetPathGenerateService(executor);
        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("id", id);
        dm.add("expr", expr);
        return executor.execute(service, dm.root()).value("path");
    }
}
