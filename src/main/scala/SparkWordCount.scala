

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Mayanka on 09-Sep-15.
 * Modified by Jordan Larson on 23-Sep-15
 */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\Jordan\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("src/main/resources/sample.txt")

    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()
    SocketClient.sendCommandToRobot(output.count()+" Total words")
    SocketClient.sendCommandToRobot("I am done.")
    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
