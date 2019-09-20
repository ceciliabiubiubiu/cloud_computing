package m_r;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input record format
 * dog -> {48889082718@N01, 48889082718@N01, 3423249@N01}
 *
 * Output for the above input key valueList
 * dog -> 48889082718@N01=2,3423249@N01=1,
 * 
 * @author Ying Zhou
 *
 */
public class Reducer extends Reducer<String, String, String, Double> {
	

	public void reduce(String key, Iterable<String> strs,  
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, String> dif_video = new HashMap<String,String>();
		Map<String, String> dif_id = new HashMap<String, String>();
		

		int count_country=0;
		int count_category=1;
		double count_result= 0.0;


		for(String s: strs){
			String same=s;
			if(dif_video.containsKey(same)){

			}else{
				dif_video.put(same, same);
			}
		}

		count_country= dif_video.size();

		for(String s: strs){
			String same=s;
			String[] dataArray=same.split("_");
			String video_id=dataArray[0];

			if(dif_id.containskey(video_id)){

			}else{
				dif_id.put(video_id, video_id);
			}
		}

		count_category= dif_id.size();
	

		count_result= count_country/count_category;


		context.write(key,count_result);

}

}







