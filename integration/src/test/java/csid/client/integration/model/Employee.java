package csid.client.integration.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;

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

    public Employee(Map<String, String> values) {
        ID = values.get("ID");
        Name = values.get("Name");
        Age = Integer.parseInt(values.get("Age"));
        SSN = Integer.parseInt(values.get("SSN"));
        Email = values.get("Email");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee that = (Employee) o;
        return Objects.equals(ID, that.ID) &&
                Objects.equals(Name, that.Name) &&
                Objects.equals(Email, that.Email) &&
                SSN == that.SSN && Age == that.Age;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ID, Name, Email, Age, SSN);
    }
}
