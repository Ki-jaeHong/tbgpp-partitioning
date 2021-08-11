import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

import java.nio.file.{Paths, Files}
import scala.collection.mutable.Map
//import org.scalatest.Assertions.assert


object TbgppApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TBGPP-copartition").setMaster("local[2]")  // TODO change
    val sc = new SparkContext(conf)

    // parse arguments
    require( args.size == 4 )
    val partitionSize:Int = args(0).toInt
    val p2pFileName = args(1)
    val p2cFileName = args(2)
    val outputPath = args(3)
    require( ! Files.exists( Paths.get(outputPath) ) )
    require( partitionSize >= 3 ) // TODO size <= 2 results to data skew

    // load two textFiles and parse
    val P2P = sc.textFile( p2pFileName ).map( x => { val l = x.split("\t"); ( l(0).toLong, l(1).toLong ) } ).cache()
    val P2C = sc.textFile( p2cFileName ).map( x => { val l = x.split("\t"); ( l(0).toLong, l(1).toLong ) } ).cache()

    // merge join predicates and transform item (sid, (did, cid)) => (cid, (sid, did))
    val P = P2P.join(P2C).map( x => (x._2._2, (x._1, x._2._1)) )

    // repartition based on hash to cid, and cache
    // since one person is located in one city, item with same sid relies on same partition
    val partFunc = new HashPartitioner(partitionSize)
    val partEdges = P.partitionBy( partFunc ).cache()
    if ( partEdges.getNumPartitions != partitionSize ) {
      throw new Exception("Wrong # partitions")
    }

    // foreach partition, accumulate list of all edges
    // foreachPartition is used for order preserving. Also distinct operator does not break ordering
    val sidListAccum = sc.collectionAccumulator[List[Long]]("sidAccum")
    val didListAccum = sc.collectionAccumulator[List[Long]]("didAccum")
    partEdges.foreachPartition( x => {
      sidListAccum.add( x.map( y => y._2._1 ).toList.distinct )
    } )
    partEdges.foreachPartition( x => {
      didListAccum.add( x.map( y => y._2._2 ).toList.distinct )
    })
    var sidList:List[Long] = Nil
    var didList:List[Long] = Nil
    sidListAccum.value.forEach( sidList ++= _ )
    didListAccum.value.forEach( didList ++= _ )

    // retain ordered list of vertices.
    // the second term is for vertices that do not occur in sid, which are assigned latter vids.
    val orderedVerticesList = sidList ++ ( didList.toSet diff sidList.toSet ).toList
    val verticesCnt = orderedVerticesList.size
    var vidCnt:Long = 0 // TODO does vid starts from 0?
    val vidMap = collection.mutable.Map[Long,Long]()
    orderedVerticesList.foreach( v => {
      vidMap += (v -> vidCnt) ;
      vidCnt += 1
    } )
    require( verticesCnt == vidMap.size )

    // broadcast map
    val broadcastedMap = sc.broadcast( vidMap.toMap )

    // change sid and did
    val renumberedPartEdges = partEdges.map( x => {
      val sid = x._2._1;
      val did = x._2._2;
      ( broadcastedMap.value(sid), broadcastedMap.value(did) )
    } )

    // print partitions
    // val output = renumberedPartEdges.glom().collect()

    // output.foreach( part => {
    //   println("----");
    //   part.foreach( x => println( x.toString ) ) ;
    //   println("----");
    // } )
    // println("done")

    // save partitions
    val edgesToStr = renumberedPartEdges.map( t => s"${t._1}	${t._2}" ) // "v\tv"
    edgesToStr.saveAsTextFile( outputPath )

    // stop spark context
    sc.stop()
  }
}