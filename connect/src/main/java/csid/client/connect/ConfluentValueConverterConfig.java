package csid.client.connect;

import csid.client.SerializationTypes;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * SmartValueConverterConfig
 */
public class ConfluentValueConverterConfig extends AbstractConfig {

    public static final String TYPE = "type";
    public static final String TYPE_DOC = "The type of the converter to use. Can be one of: `PROTOBUF`, `JSON`, `AVRO`.";

    private static final ConfigDef configDef = new ConfigDef().define(TYPE, ConfigDef.Type.STRING, SerializationTypes.Avro.toString(), ConfigDef.Importance.HIGH, TYPE_DOC);

    public ConfluentValueConverterConfig(Map<?, ?> originals) {
        super(configDef, originals, true);
    }

    public SerializationTypes getType() {
        return SerializationTypes.fromString(getString(TYPE));
    }
}
