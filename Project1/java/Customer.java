package Java;
import java.lang.String;
public class Customer {
	private int ID;
	private String Name;
	private int Age;
	private String Gender;
	private int CountryCode;
	private float Salary;
	
	public Customer(int id, String name, int age, String gender, int countrycode, float salary){
		this.ID = id;
		this.Name = name;
		this.Age = age;
		this.Gender = gender;
		this.CountryCode = countrycode;
		this.Salary = salary;
	}
	
	public String write(){	
		return String.valueOf(ID) + "," + Name + ","+ String.valueOf(Age) + ","
				+ Gender + "," + String.valueOf(CountryCode) + "," + String.valueOf(Salary)+"\n";
	}
	
}
