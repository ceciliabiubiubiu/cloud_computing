package video;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.*;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

public class Video_Reducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> strs,
                        Context context
        ) throws IOException, InterruptedException {

                // create a map to remember the owner frequency
                // keyed on owner id
                Map<String, String> dif_video = new HashMap<String,String>();
                Map<String, String> dif_id = new HashMap<String, String>();



                for(Text s: strs){
                        String same=s.toString();
                        if(dif_video.containsKey(same)){

                        }else{
                                dif_video.put(same, same);
                        }
                        String[] dataArray = same.split(",");
                        String video_id = dataArray[0];
                        if(dif_id.containsKey(video_id)){

                        }else{
                                dif_id.put(video_id, video_id);
                        }
                }

                int count_country= dif_video.size();

                int count_category= dif_id.size();

                double count_result= ((double) count_country)/count_category;
                DecimalFormat df = new DecimalFormat("#.00");
                String count_result_dou=df.format(count_result);
                context.write(key,new Text(count_result_dou + ""));

}

}

