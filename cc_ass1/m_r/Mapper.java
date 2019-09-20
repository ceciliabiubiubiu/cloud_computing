package m_r;



import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 * 
 * output key value pairs for the above input
 * dog -> 48889082718@N01
 * francis -> 48889082718@N01
 * 
 * @author Ying Zhou
 *
 */
//先写吧，写完之后再研究书好了

public class Mapper extends Mapper<Object, Text, String, String> {
	


	
	// a mechanism to filter out non ascii tags
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(String key, String value, Context context
	) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split(","); //split the data into array
		if (dataArray.length < 12){ //  record with incomplete data
			return; // don't emit anything
		}


		String video_id=dataArray[0];
		String category=dataArray[3];
		String country=dataArray[11];

		context.write(category, video_id+"_"+country);
	}

}




