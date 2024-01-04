/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package confluent.client.serializer.record;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

import java.util.Objects;

@JsonSchemaInject(strings = {@JsonSchemaString(path = "javaType",
        value = "Record.OrderSchemaRecord$OrderSchemaRecord")})
public class OrderSchemaRecord {

    @JsonProperty
    public String name;
    @JsonProperty
    public String orderRef;
    @JsonProperty
    public String customer;
    @JsonProperty
    public String customerCode;

    public OrderSchemaRecord() {
    }

    public OrderSchemaRecord(String name, String orderRef, String customer, String customerCode) {
        this.name = name;
        this.orderRef = orderRef;
        this.customer = customer;
        this.customerCode = customerCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderSchemaRecord orderSchemaRecord = (OrderSchemaRecord) o;
        return Objects.equals(name, orderSchemaRecord.name)
                && Objects.equals(orderRef, orderSchemaRecord.orderRef)
                && Objects.equals(customer, orderSchemaRecord.customer)
                && Objects.equals(customerCode, orderSchemaRecord.customerCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, orderRef, customer, customerCode);
    }

}