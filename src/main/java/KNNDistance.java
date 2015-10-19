import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by shanmukh on 10/18/15.
 */
public class KNNDistance implements Serializable, Comparator<KNNDistance> {
    public long rowId;
    public double distance;
    public int distFromClass ;
    public int compare(KNNDistance d1, KNNDistance d2) {
            if ( d1.distance < d2.distance){
                return 1 ;
            }
            else if ( d1.distance > d2.distance){
                return -1;
            }
            else{
                return 0;
            }
    }
}
