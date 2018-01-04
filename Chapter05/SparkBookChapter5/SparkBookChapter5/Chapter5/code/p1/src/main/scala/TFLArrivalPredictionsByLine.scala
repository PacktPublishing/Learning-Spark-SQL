import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.jfarcand.wcs.{TextListener, WebSocket}
import scala.util.parsing.json.JSON
import scalaj.http.Http

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
/**
* Spark Streaming Example TfL Receiver from Slack
*/
class TFLArrivalPredictionsByLine() extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable {
  private val tflUrl = "https://api.tfl.gov.uk/Line/circle/Arrivals?stopPointId=940GZZLUERC&app_id=a73727f3&app_key=dc8150560a2422afae2b70cf291c4327"
  @transient
  private var thread: Thread = _
  override def onStart(): Unit = {
     thread = new Thread(this)
     thread.start()
  }
  override def onStop(): Unit = {
     thread.interrupt()
  }
  override def run(): Unit = {
    while (true){
     receive();
     Thread.sleep(60*1000);
    }
   }
  
  private def receive(): Unit = {
    val httpClient = new DefaultHttpClient();
    val getRequest = new HttpGet(tflUrl);
    getRequest.addHeader("accept", "application/json");

    val response = httpClient.execute(getRequest);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
         + response.getStatusLine().getStatusCode());
    }

    val br = new BufferedReader(
                         new InputStreamReader((response.getEntity().getContent())));

    var output=br.readLine();
    while(output!=null){          
          println(output)
          output=br.readLine()
        }    
  }
}

