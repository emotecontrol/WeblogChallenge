import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val sourceFile = "src/main/resources/2015_07_22_mktplace_shop_web_log_sample.log"
    val conf = new SparkConf().setAppName("SparkWebLog").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ss = SparkSession.builder().appName("SparkWebLog").getOrCreate()

    import ss.sqlContext.implicits._

    val csv = ss.read.option("sep"," ").csv(sourceFile)

    val dataNames = Seq("timestamp", "location", "sourceip", "destinationip", "duration1", "duration2", "duration3", "http1", "http2", "http3", "http4", "request", "browser", "encoding", "tls")

    // Assumption: a 60-minute window will characterize a session
    // Assumption: a sliding window will more completely capture sessions at the expense of possibly capturing sessions more than once
    // Fun: the window duration and sliding interval can be played around with here to experiment with various settings
    val windowDuration = "60 minutes"
    val windowInterval = "15 minutes"

    // Assumption: A user is defined by an IP address, disregarding the port

    val data = csv.toDF(dataNames: _*)
      .withColumn("time", $"timestamp".cast("timestamp")).drop("timestamp").withColumnRenamed("time", "timestamp")
      .withColumn("longtime", $"timestamp".cast("long"))
      .withColumn("user", {
        udf(dropPorts).apply($"sourceip")
      })

    // Get the earliest and latest requests by each user within a 1-hour window, shifted by 15-min intervals
    // Calculate the length of user sessions, and also note number of requests
    val sessions = data.groupBy($"user", window($"timestamp", windowDuration, windowInterval))
    val engagedUsers = sessions.agg(min("longtime"), max("longtime"), count("longtime"))
      .withColumnRenamed("max(longtime)", "last request in session (epoch)")
      .withColumnRenamed("min(longtime)", "earliest request in session (epoch)")
      .withColumnRenamed("count(longtime)", "number of requests in session")
      .withColumn("time difference (seconds)", $"last request in session (epoch)" - $"earliest request in session (epoch)")
      .orderBy($"time difference (seconds)".desc, $"number of requests in session".desc)

    // Get the longest session for each user and format the session length to make it readable
    val mostEngagedUsers = engagedUsers
      .groupBy($"user")
      .agg(max($"time difference (seconds)"))
      .withColumnRenamed("max(time difference (seconds))", "time in session (seconds)")
      .withColumn("time in session", {
        val seconds = format_string("%02d", $"time in session (seconds)".mod(60))
        val minutes = format_string("%02d", ($"time in session (seconds)" - seconds).divide(60).mod(60).cast("int"))
        val hours = format_string("%02d", ($"time in session (seconds)" - (seconds + minutes.multiply(60))).divide(3600).cast("int"))
        concat(hours, lit(":"), minutes, lit(":"), seconds)
      })
      .orderBy($"time in session (seconds)".desc)

    // Get the average session length
    def avgSessionTime = engagedUsers
      .agg(avg($"time difference (seconds)"))
      .withColumnRenamed("avg(time difference (seconds))", "average session time (seconds)")

    // Assumption: a unique URL does not include parameters
    def formatUrl = (rawUrl: String) => {
      val urlMatcher = """\w*\s*http[s]?:\/\/(\w*.\w*:\d*[a-zA-Z0-9\/_\-\.]*)""".r
      // Captures urls beginning with "http" but not including GET params starting with ?
      // Also drops the GET or POST at the beginning of a request field.
      val capture = urlMatcher.findFirstMatchIn(rawUrl)
      for (m <- capture) yield m.group(1)
    }

    // Get all url requests, grouped by window, from request field
    // Trim extra information from urls
    def urlHits = data
      .withColumn("url", udf(formatUrl).apply($"request"))
      .groupBy(window($"timestamp", windowDuration, windowInterval))

    // Get a count of distinct url requests per window
    def hitsCount = urlHits.agg(countDistinct($"url"))
      .withColumnRenamed("count(DISTINCT url)", "unique url hits")
      .orderBy($"window")

    // Output sample data
    println("Average session time")
    avgSessionTime.show(1)
    println("Number of unique hits per session window")
    hitsCount.show(30, false)
    println("Most engaged users (based on session time)")
    mostEngagedUsers.show(10)

  }

  def dropPorts: String => String = (address: String) => {
    address.takeWhile(c => c != ':')
  }

}
