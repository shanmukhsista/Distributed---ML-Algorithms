import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shanmukh on 10/18/15.
 */
public class DataRow implements Serializable {
    int predictedClassLabel = -99;
    String type ;
    int id ;
    public DataRow(){
        classLabel = -1 ;
        columns = new ArrayList<Double>();
    }
    public List<Double> columns ;
    public int classLabel;
}
