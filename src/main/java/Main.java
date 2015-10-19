/**
 * Created by shanmukh on 10/17/15.
 */
import akka.actor.FSM;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.logging.LogManager;

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
public class Main implements Serializable{

    public static void main(String[] args)  throws Exception{
        final PrintWriter pw = new PrintWriter("/Users/shanmukh/results.csv");
        //Write headers.
        pw.println("\"Actual Class\",\"Predicted Class\", \"Distance From Class\"");
        SparkConf conf = new SparkConf().setAppName("Distributed Decision Trees").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<DataRow> initialTrainData= ReadTrainingFile(sc, "zoo-train.csv", "train");
        JavaRDD<DataRow> trainingData = NormalizeData(initialTrainData);
        JavaRDD<DataRow> initialTestingData = ReadTrainingFile(sc, "zoo-test.csv", "test");
        JavaRDD<DataRow> testingData = NormalizeData(initialTestingData);

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
            pw.println( dr.classLabel + "," + orderedL.get(0).distFromClass + ","  + orderedL.get(0).distance);
            //System.out.println("Distance Points " + distancePoints.takeOrdered(1).get(0).distance);
            //Our prediction is minimum distance ( Closest neighbor )
             }
        pw.close();
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
                    row.columns.add(Double.parseDouble(cols[i]));
                }
                return row ;
            }
        });
        System.out.println(type + " has " + distances.count() + " rows " );
        return  distances;
    }
    public static JavaRDD<DataRow> NormalizeData(JavaRDD<DataRow> rows ){
        JavaPairRDD<Integer, Double> splitRows  = rows.flatMapToPair(new PairFlatMapFunction<DataRow, Integer, Double>() {
            public Iterable<Tuple2<Integer, Double>> call(DataRow dataRow) throws Exception {
                List<Tuple2<Integer, Double>> colValues = new ArrayList<Tuple2<Integer, Double>>();
                for (int i = 0; i < dataRow.columns.size(); i++) {
                    colValues.add(new Tuple2(i, (double)dataRow.columns.get(i)));
                }
                return colValues;
            }
            //we have a pari rdd with key as col index and value.
            //find max value for each column.

        }).cache();
        JavaPairRDD<Integer, Double> maxValues = splitRows.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                if (aDouble > aDouble2) {
                    return aDouble;
                } else {
                    return aDouble2;
                }
            }

        });
        final Map<Integer, Double> maxValuesMap = maxValues.collectAsMap();
        JavaPairRDD<Integer, Double> minValues = splitRows.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                if (aDouble < aDouble2) {
                    return aDouble;
                } else {
                    return aDouble2;
                }
            }
        });
        final Map<Integer, Double> minValuesMap = minValues.collectAsMap();
        System.out.println(minValuesMap);
        //Normalize these values now.

       JavaRDD<DataRow> normalizedValues =  rows.map(new Function<DataRow, DataRow>() {
            public DataRow call(DataRow dataRow) throws Exception {
                List<Double> cols = dataRow.columns;
                List<Double> newCols = new ArrayList<Double>();
                for (int i = 0; i < dataRow.columns.size() ; i++) {
                    double nValue = 0.0;
                    nValue = ((cols.get(i)- minValuesMap.get(i))/(maxValuesMap.get(i) - minValuesMap.get(i))) ;
                    newCols.add(i, nValue);
                }
                dataRow.columns = newCols;
                return dataRow;
            }
        });
        return normalizedValues;
    }


}
class DistanceComparator implements  Comparator<KNNDistance>, Serializable{

    public int compare(KNNDistance d1, KNNDistance d2) {
        if ( d1.distance < d2.distance){
            return -1 ;
        }
        else if ( d1.distance > d2.distance){
            return 1;
        }
        else{
            return 0;
        }
    }
}