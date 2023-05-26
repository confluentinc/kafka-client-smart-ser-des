package csid.client.connect;

/**
 * SmartValueConverterTypes
 */
public enum ConfluentValueConverterTypes {
    PROTOBUF,
    AVRO,
    JSON;

    public static ConfluentValueConverterTypes fromString(String value) {
        for (ConfluentValueConverterTypes type : ConfluentValueConverterTypes.values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }

        return PROTOBUF;
    }
}
