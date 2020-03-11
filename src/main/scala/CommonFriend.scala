import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class AvgCollector(val tot: Double, val cnt: Int = 1) extends Serializable{
  def combine(that: AvgCollector) = new AvgCollector(tot + that.tot, cnt + that.cnt)
  def avg = tot / cnt
}
class StrCollector(val in: String, val cnt: Int = 1) extends Serializable{
  def combine(that: StrCollector) = new StrCollector(in + ","+that.in, cnt + that.cnt)
  def out:String = in
}

object CommonFriend {

  def commonFriend(){

    val conf = new SparkConf().setAppName("common firend").setMaster("local[1]")
    /* spark context*/
    val sc = new SparkContext(conf)
    /* map */
    var input = sc.textFile("../inPutMapReduce/soc-Livejornal1adj.txt")
    var map = input.map(line => line.split("\\t")).filter(line=>(line.length == 2)).flatMap(line =>({
      val list = line(1).split(",");
      list.map(x=>{
        if(line(0) < x) ((line(0),x),line(1))
        else ((x,line(0)),line(1))
      })
    }) )

    /* reduce */
    var reduce = map.map(x=>((x._1._1, x._1._2), x._2.split(",").toSet))
    .reduceByKey((a,b)=>a.intersect(b)).sortByKey().map(x=>(x._1 + "\t"+x._2.size))

    /* or save the output to file */
    reduce.saveAsTextFile("Q1out.txt")

    sc.stop()
  }

  def topTenCommonFriend(): Unit ={
    val conf = new SparkConf().setAppName("top 10 common firend").setMaster("local[1]")
    /* spark context*/
    val sc = new SparkContext(conf)
    var friend_input = sc.textFile("../inPutMapReduce/soc-Livejornal1adj.txt")
    var user_data_input = sc.textFile("../inPutMapReduce/userdata.txt")

    var commonmap = friend_input.map(line => line.split("\\t")).filter(line=>(line.length == 2)).flatMap(line => {
      val list = line(1).split(",");
      list.map(x=>{
        if(line(0) < x) ((line(0),x),line(1))
        else ((x,line(0)),line(1))
      })
    } )

    /* reduce */
    var commonreduce = commonmap.map(x=>((x._1._1, x._1._2), x._2.split(",").toSet))
    .reduceByKey((a,b)=>a.intersect(b)).sortByKey().map(x=>(x._1._1,x._1._2,x._2.size))

    var data_map = user_data_input.map(line=>line.split(",")).map(x=>(x(0),(x(1),x(2),x(3)))).collect.toMap

    var friend_map = commonreduce.map(line=>(line._3,data_map.get(line._1).get,data_map.get(line._2).get)).sortBy(-_._1)

//    <Total number of Common Friends><TAB><First Name of User A><TAB><Last Name of User A> <TAB><address of User A><TAB><First Name of User B><TAB><Last Name of User B><TAB><address of User B>
    var friend_reduce = sc.parallelize(friend_map
                                       .map(x=>(x._1+"\t"+x._2._1+"\t"+x._2._2+"\t"+x._2._3+"\t"+x._3._1+"\t"+x._3._2+"\t"+x._3._3))
                                       .take(10))

    friend_reduce.saveAsTextFile("Q2out.txt")

    sc.stop()
  }

  def reviewInStandford(): Unit ={
    //review n standford
    val conf = new SparkConf().setAppName("review in standford ").setMaster("local[1]")
    /* spark context*/
    val sc = new SparkContext(conf)
    var bussiness_input = sc.textFile("business.csv")
    var review_input = sc.textFile("review.csv")
    //standford zip code 94305,94309
    val regex = """\bStanford""".r
    //hash map
    var bussiness_map = bussiness_input.map(line=>line.split("::")).map(x=>(x(0),x(1))).collect.toMap
  //.filter(line=>regex.findFirstIn(line(1)).isDefined)

    var review_map = review_input.map(line=>line.split("::"))
    .map(x=>(x(0),x(1),bussiness_map.get(x(2)).get,x(3))).filter(line=>regex.findFirstIn(line._3).isDefined)
    .map(x=>(x._2,x._4))
    var reduce = review_map.map{case(a,b)=> (a,new StrCollector(b))}.reduceByKey( _ combine _ )
    .map{case(a,b)=>List(a,b.out).mkString("::")}

    reduce.saveAsTextFile("Q3out.txt")

    sc.stop()

  }

  def avgRrating(): Unit ={
    val conf = new SparkConf().setAppName("review in standford ").setMaster("local[1]")
    /* spark context*/
    val sc = new SparkContext(conf)
    var bussiness_input = sc.textFile("business.csv")
    var review_input = sc.textFile("review.csv")

    var bussiness_map = bussiness_input.map(line=>line.split("::")).map(x=>(x(0),(x(1),x(2)))).collect.toMap

    //hash map
    var review_map = review_input.map(line=>line.split("::")).map(x=>(x(2),x(3).toDouble))
    var review_reduce = sc.parallelize(review_map.map{case(a,b)=> (a,new AvgCollector(b)) }
                                       .reduceByKey( _ combine _ ).map{case(x,y)=>(x,bussiness_map.get(x).get._1,bussiness_map.get(x).get._2,y.avg)}
    .sortBy(-_._4).take(10).map(x=>List(x._1,x._2,x._3,x._4).mkString("::")))

    review_reduce.saveAsTextFile("Q4out.txt")

    sc.stop()
  }

  def main(args:Array[String]): Unit ={

    commonFriend();
    topTenCommonFriend();
    reviewInStandford();
    avgRrating();

  }

}
