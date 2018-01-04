import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TFLCustomReceiver {
  private val url = "https://api.tfl.gov.uk/Line/circle/Arrivals?stopPointId=940GZZLUERC&app_id=a73727f3&app_key=dc8150560a2422afae2b70cf291c4327"
  def main(args: Array[String]) {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("TFLCustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(300))
    
    val lines = ssc.receiverStream(new TFLCustomReceiver(url))
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

class TFLCustomReceiver(url: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Http Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  
  private def receive() {
    var userInput: String = null
    var httpClient: DefaultHttpClient = null
    var getRequest: HttpGet = null
    
    try {
     // Connect to host:port
     httpClient = new DefaultHttpClient();
     getRequest = new HttpGet(url);
     getRequest.addHeader("accept", "application/json");

     while(!isStopped) {
        val response = httpClient.execute(getRequest);
        if (response.getStatusLine().getStatusCode() != 200) {
                        throw new RuntimeException("Failed : HTTP error code : "+ response.getStatusLine().getStatusCode());
        }
        val reader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
        userInput = reader.readLine()
        while(userInput != null) {
           store(userInput)
          //println(userInput)
          userInput = reader.readLine()
        }
       reader.close()
       Thread.sleep(60*1000)
     }
     httpClient.close()
     // Restart in an attempt to connect again when server is active again
     //restart("Trying to connect again")
    } catch {
     case e: java.net.ConnectException =>
       // restart if could not connect to server
       restart("Error connecting to " + url, e)
     case t: Throwable =>
       // restart if there is any other error
       restart("Error receiving data", t)
    }
  }

}