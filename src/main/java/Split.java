import java.util.ArrayList;
import java.util.List;


public class Split implements Cloneable{
	public List featureIndex;
	public List featureValue;
	String classLabel;
	public Split()
	{
		 this.featureIndex = new ArrayList<Integer>();
		 this.featureValue = new ArrayList<String>();
	}
	public Split(List featureIndex, List featureValue)
	{
		this.featureIndex = featureIndex;
		this.featureValue =featureValue;
	}
}
