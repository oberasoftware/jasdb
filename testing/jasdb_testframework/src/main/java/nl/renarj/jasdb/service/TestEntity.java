package nl.renarj.jasdb.service;

import com.oberasoftware.jasdb.api.entitymapper.annotations.Id;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 */
@JasDBEntity(bagName = "TEST_BAG")
public class TestEntity {
    private String id;
    private String firstName;
    private String lastName;

    private List<String> hobbies;

    private Map<String, String> addressProperties;

    public TestEntity(String id, String firstName, String lastName, List<String> hobbies, Map<String, String> addressProperties) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.hobbies = hobbies;
        this.addressProperties = addressProperties;
    }

    public TestEntity() {
    }

    @Id
    @JasDBProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JasDBProperty
    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JasDBProperty
    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @JasDBProperty(name = "HobbyList")
    public List<String> getHobbies() {
        return hobbies;
    }

    public void setHobbies(List<String> hobbies) {
        this.hobbies = hobbies;
    }

    @JasDBProperty(name = "Address")
    public Map<String, String> getAddressProperties() {
        return addressProperties;
    }

    public void setAddressProperties(Map<String, String> addressProperties) {
        this.addressProperties = addressProperties;
    }
}
