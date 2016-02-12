import java.util.ArrayList;
import java.util.List;


public class Split implements Cloneable{
	public List att_index;
	public List att_value;
	String classLabel;
	public Split()
	{
		 this.att_index= new ArrayList<Integer>();
		 this.att_value = new ArrayList<String>();
	}
	public Split(List att_index, List att_value)
	{
		this.att_index=att_index;
		this.att_value=att_value;
	}
	
	public void add(Split obj)
	{
		this.add(obj);
	}
}
