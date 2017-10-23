package daris.plugin.sink;

import java.util.LinkedHashMap;
import java.util.Map;

import arc.mf.plugin.DataSinkImpl;
import arc.mf.plugin.dtype.DataType;
import arc.mf.plugin.dtype.StringType;
import arc.mf.plugin.sink.ParameterDefinition;

public abstract class AbstractDataSink implements DataSinkImpl {

    private Map<String, ParameterDefinition> _paramDefns;
    private String _typeName;

    public AbstractDataSink(String typeName) throws Throwable {
        if (typeName.startsWith(SinkConstants.SINK_TYPE_NAME_PREFIX)) {
            _typeName = typeName;
        } else {
            _typeName = SinkConstants.SINK_TYPE_NAME_PREFIX + typeName;
        }
        _paramDefns = new LinkedHashMap<String, arc.mf.plugin.sink.ParameterDefinition>();
        addParameterDefinitions(_paramDefns);
        _paramDefns.put(SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT,
                new ParameterDefinition(StringType.DEFAULT, SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT_DESCRIPTION));
    }

    protected abstract void addParameterDefinitions(Map<String, ParameterDefinition> paramDefns) throws Throwable;

    protected void addParameterDefinition(Map<String, ParameterDefinition> paramDefns, String name, DataType type,
            String description, Boolean assetSpecific) throws Throwable {
        String pn = name.toLowerCase();
        if (assetSpecific != null && assetSpecific) {
            if (!pn.startsWith(SinkConstants.SINK_ARG_ASSET_SPECIFIC_PREFIX)) {
                pn = SinkConstants.SINK_ARG_ASSET_SPECIFIC_PREFIX + pn;
            }
        }
        if (paramDefns.containsKey(pn)) {
            throw new IllegalArgumentException("Argument: " + pn + " for sink type: " + type() + " already exists.");
        }
        paramDefns.put(name, new ParameterDefinition(type, description));
    }

    @Override
    public final String type() throws Throwable {
        return _typeName;
    }

    @Override
    public final Map<String, ParameterDefinition> parameterDefinitions() throws Throwable {
        return _paramDefns;
    }

    protected String getAssetSpecificOutput(Map<String, String> params) {
        return params == null ? null : params.get(SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT);
    }
}
