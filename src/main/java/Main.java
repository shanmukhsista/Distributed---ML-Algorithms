/**
 * Created by shanmukh on 10/17/15.
 */
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.*;

class byValue implements Comparator<Map.Entry<String,Integer>> {
    public int compare(Map.Entry<String,Integer> e1, Map.Entry<String,Integer> e2) {
        if (e1.getValue() < e2.getValue()){
            return 1;
        } else if (e1.getValue() == e2.getValue()) {
            return 0;
        } else {
            return -1;
        }
    }
}
public class Main implements Serializable {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Distributed Decision Trees").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<DataRow> trainingData= ReadTrainingFile(sc, "zoo-train.csv", "train");
        JavaRDD<DataRow> testingData = ReadTrainingFile(sc, "zoo-test.csv", "test");
        for ( DataRow  dr:  testingData.take((int) testingData.count())){
            final DataRow dr1 = dr;
            System.out.println("ROw "+ dr.classLabel + " Class label ; " + dr.classLabel);
            JavaRDD<KNNDistance> distancePoints = trainingData.map(new Function<DataRow, KNNDistance>() {
                public KNNDistance call(DataRow dataRow) throws Exception {
                    double dist = 0.0;
                    for (int j = 0; j < dr1.columns.size(); j++) {
                        dist += Math.pow((dataRow.columns.get(j) - dr1.columns.get(j)), 2);
                    }
                    //This is the distance for that row.
                    dist = Math.sqrt(dist);
                    KNNDistance d = new KNNDistance();
                    d.distFromClass = dataRow.classLabel ;
                    d.distance = dist;
                    return d ;
                }
            });
            for ( KNNDistance d : distancePoints.collect()){
                //System.out.println(d.distance  + " - Prediction class : " + d.distFromClass + " Actual Class : "  +dr.classLabel) ;
            }
            List<KNNDistance> orderedL = distancePoints.takeOrdered(1, new DistanceComparator());
            System.out.println(orderedL.get(0).distance + " Prediction = " + orderedL.get(0).distFromClass + " Actual Class " + dr.classLabel);
            //System.out.println("Distance Points " + distancePoints.takeOrdered(1).get(0).distance);
            //Our prediction is minimum distance ( Closest neighbor )
             }
    }

    public static JavaRDD<DataRow> ReadTrainingFile(JavaSparkContext sc , String fileName, final String type){
        //Read entire file as RDD of strings.
        JavaRDD<String> lines = sc.textFile(fileName);
        JavaRDD<DataRow> distances = lines.map(new Function<String, DataRow>() {
            public DataRow call(String s) throws Exception {
                //Split string by ,
                String[] cols = s.split(",");
                int classLabel = Integer.parseInt(cols[cols.length -1 ]);
                DataRow row = new DataRow();
                row.classLabel = classLabel;
                row.id = cols.hashCode();
                row.type = type;
                for ( int i =0 ; i < cols.length -2 ; i++){
                    row.columns.add(Integer.parseInt(cols[i]));
                }
                return row ;
            }
        });
        System.out.println(type + " has " + distances.count() + " rows " );
        return  distances;
    }


}
class DistanceComparator implements  Comparator<KNNDistance>, Serializable{

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