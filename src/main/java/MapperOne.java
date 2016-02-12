import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * Created by hadoop on 31.01.16.
 */
public class MapperOne implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private int i;
    private String token;
    public static int no_Attr;
    //public static int splitAttr[];
    private int flag=0;
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        TreeBuilder id=new TreeBuilder();
        int size_split=0;
        Split split=id.currentSplit;
        StringTokenizer itr = new StringTokenizer(value);
        int index=0;
        String att_value=null;
        no_Attr=itr.countTokens()-1;
        String attr[]=new String[no_Attr];
        boolean match=true;
        for(i =0;i<no_Attr;i++)
        {
            attr[i]=itr.nextToken();		//Finding the values of different attributes
        }
        String classLabel=itr.nextToken();
        size_split=split.att_index.size();
        for(int count=0;count<size_split;count++)
        {
            index=(Integer) split.att_index.get(count);
            att_value=(String)split.att_value.get(count);
            if(attr[index].equals(att_value))   //may also use attr[index][z][1].contentEquals(att_value)
            {}
            else
            {
                match=false;
                break;
            }

        }
        if(match)
        {
            for(int l=0;l<no_Attr;l++)
            {
                if(split.att_index.contains(l))
                {

                }
                else
                {
                    token=l+" "+attr[l]+" "+classLabel;
                    out.collect(new Tuple2<String, Integer>(token,1));
                }

            }
            if(size_split==no_Attr)
            {
                token=no_Attr+" "+"null"+" "+classLabel;
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
