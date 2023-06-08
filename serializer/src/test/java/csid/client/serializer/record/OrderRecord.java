/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package csid.client.serializer.record;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class OrderRecord {


    @JsonProperty
    public String name;
    @JsonProperty
    public String orderRef;
    @JsonProperty
    public String customer;
    @JsonProperty
    public String customerCode;

    public OrderRecord() {
    }

    public OrderRecord(String name, String orderRef, String customer, String customerCode) {
        this.name = name;
        this.orderRef = orderRef;
        this.customer = customer;
        this.customerCode = customerCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderRecord that = (OrderRecord) o;
        return Objects.equals(name, that.name) && Objects.equals(orderRef, that.orderRef) && Objects.equals(customer, that.customer) && Objects.equals(customerCode, that.customerCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, orderRef, customer, customerCode);
    }
}
