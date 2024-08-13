/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.serializer.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
@NoArgsConstructor
public class Employee {

    @JsonProperty(value = "ID", index = 1)
    public String ID;

    @JsonProperty(value = "Name", index = 2)
    public String Name;

    @JsonProperty(value = "Age", index = 3)
    public int Age;

    @JsonProperty(value = "SSN", index = 4)
    public int SSN;

    @JsonProperty(value = "Email", index = 5)
    public String Email;

    @JsonIgnore
    public boolean isValid() {
        return StringUtils.isNotEmpty(ID) && StringUtils.isNotEmpty(Name) && StringUtils.isNotEmpty(Email) && Age > 0 && SSN > 0;
    }
}
