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

    public AbstractDataSink(String typeName) {
        if (typeName.startsWith(SinkConstants.SINK_TYPE_NAME_PREFIX)) {
            _typeName = typeName;
        } else {
            _typeName = SinkConstants.SINK_TYPE_NAME_PREFIX + typeName;
        }
        _paramDefns = new LinkedHashMap<String, ParameterDefinition>();
    }

    protected void addParameterDefinition(String name, DataType type, String description, boolean assetSpecific)
            throws Throwable {
        String pn = name.toLowerCase();

        if (assetSpecific) {
            if (!pn.startsWith(SinkConstants.SINK_ARG_ASSET_SPECIFIC_PREFIX)) {
                pn = SinkConstants.SINK_ARG_ASSET_SPECIFIC_PREFIX + pn;
            }
        }
        if (_paramDefns.containsKey(pn)) {
            throw new IllegalArgumentException("Argument: " + pn + " for sink type: " + type() + " already exists.");
        }
        _paramDefns.put(name, new ParameterDefinition(type, description));
    }

    protected void addParameterDefinition(String name, DataType type, String description) throws Throwable {
        addParameterDefinition(name, type, description, false);
    }

    @Override
    public final String type() throws Throwable {
        return _typeName;
    }

    @Override
    public final Map<String, ParameterDefinition> parameterDefinitions() throws Throwable {
        if (!_paramDefns.containsKey(SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT)) {
            addParameterDefinition(SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT, StringType.DEFAULT,
                    SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT_DESCRIPTION, true);
        }
        return _paramDefns;
    }

    protected String getAssetSpecificOutput(Map<String, String> params) {
        return params == null ? null : params.get(SinkConstants.SINK_ARG_ASSET_SPECIFIC_OUTPUT);
    }
}
