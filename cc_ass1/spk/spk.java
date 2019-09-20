
import java.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.sql.*;








public class spk{

	public static void main(String[] args) {
		String inputDataPath = args[0], outputDataPath = args[1];

			SparkSession spark = SparkSession.builder().appName("java_video").master("local").getOrCreate();
			JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> data_needed = sc.textFile(inputDataPath);


		Date date=new Date();


		//filter





		//return了一个包含重复值的tuple2
		JavaPairRDD<String, String> info_extraction= data_needed.mapToPair(
			s ->
			{
				String[] values=s.split(",");
				String id_country=values[0]+","+values[11];
			//trending_date, category，likes, dislikes
				String info=values[1]+","+values[3]+","+values[6]+","+values[7];

				return new Tuple2<String, String>(id_country, info);
			// filter
			// 	ArrayList<String, String> array= new ArrayList<String, String>();
			// 	array.add(id_country, info);
			// 	for(int i=0; i<array.length(); i++){
			// 		for(int j=1; j<array.length(); j++){
			// 		if(array[i]._1().equals(array[j]._1()){
			// 	return Tuple2<String, String>(array[i]._1(), array[i]._2());
			// 		}
			// }
			// }

			}
			);

		info_extraction.saveAsTextFile(outputDataPath);
		sc.close();
		



// 		//这里function的传递好像不太对
// 		JavaPairRDD<String, String> same_id_country = info_extraction.aggregateByKey(0, 
// 			new Function2<Integer, Integer>(){

// 				String[] values =info.split(",");
// 				String trending_date= values[0];
// 				String category=values[1];
// 				Integer likes = Integer.valueOf(values[2]);
// 				Integer dislikes = Integer.valueOf(values[3]);

// 				//这边要有一个sort日期的方法。。。sort应该是sort所有的内容吧？？

// 				Date date_unsorted = trending_date.str_to_date;
// 				Arraylist<Tuple2<Date, String>> list = new Arraylist<>();
// 				list.add(date_unsorted, values[2]+","+values[3]);
// 				Collections.sort(list);



// //find the number
// 			public Integer call(Integer s, Integer v) throw Exception {
// 					int result= (Integer.valueOf(
// 						list([1]._2().split(",")[2]))-
// 					Integer.valueOf(list([0]._2().split(",")[2])))-
// 					(Integer.valueOf(list([1}._2().split(",")[1]))-
// 						Integer.valueOf(list[0]._2().split(",")[1]))

// 				String infor=category;



// 			return new Tuple2<Integer, String>(result, category)

// 		}
// 			}
// 			)


	}
}