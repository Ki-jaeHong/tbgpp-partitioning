import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.TaskContext


import java.nio.file.{Paths, Files}
import scala.collection.mutable.{Map => MutMap, ArrayBuffer => MutArrayBuffer}
import scala.collection.Map

import scala.math.{max, min}
import collection.JavaConverters._

//import org.scalatest.Assertions.assert

// K : type of key
// Round Robin on key occurences
class BBPPartitioner[K](numPart: Int, sortedKeyOccurences: List[K]) extends Partitioner {

  val partitionFunc:Map[K, Int] = sortedKeyOccurences.zipWithIndex.map( (tup) => ( tup._1 -> (tup._2 % numPart) ) ).toMap
  override def numPartitions: Int = numPart
  override def getPartition(key: Any): Int = {
    key match {
      case k: K      => partitionFunc(k)
      case _: Any    => 0
    } 
  }

  override def equals(other: Any): Boolean = other match {
    case partitioner: BBPPartitioner[K] =>
      partitioner.numPartitions == numPart
    case _ =>
      false
  }

}

// greedily make sizes of partitions equal
class EquiSizePartitioner[K](numPart: Int, sortedKeyOccurencesWithWeights: List[(K, Int)]) extends Partitioner {

  val accumEdges = MutArrayBuffer[Int]()
  Range(0, numPart).toList.foreach( _ => accumEdges += 0 )
  val partitionFunc = MutMap[K, Int]()

  def getMinAccumedIdx = accumEdges.zipWithIndex.sortBy( tup => tup._1 ).head._2
  sortedKeyOccurencesWithWeights.foreach( keyweight => {
    val key: K = keyweight._1
    val weight: Int = keyweight._2
    val selectedIdx: Int = getMinAccumedIdx
    accumEdges( selectedIdx ) += weight
    
    partitionFunc += (key -> selectedIdx)
  })

  override def numPartitions: Int = numPart
  override def getPartition(key: Any): Int = {
    key match {
      case k: K      => partitionFunc(k)
      case _: Any    => throw new Exception("Wrong partition key")
    } 
  }

  override def equals(other: Any): Boolean = other match {
    case partitioner: EquiSizePartitioner[K] =>
      partitioner.numPartitions == numPart
    case _ =>
      false
  }

}

object TbgppApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
                .setAppName("TBGPP-copartition")
                .setMaster("local[10]")
                .set("spark.driver.memory", "100g")
              //  .set("spark.executor.memory", "50g")  // TODO change. this is local mode config
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    // parse arguments
    require( args.size == 6 )
    val partitionSize:Int = args(0).toInt
    val p2pFileName = args(1)
    val p2cityFileName = args(2)
    val city2countryFileName = args(3)
    val conuntry2continentFileName = args(4)
    val outputPath = args(5)
    require( ! Files.exists( Paths.get(outputPath) ) )  // keep outpath empty!
    require( partitionSize >= 3 ) // TODO size <= 2 results in data skew

    // load textFiles and parse
    def parseLine(line:String) = {
      val split = line.split("\t");
      ( split(0).toLong, split(1).toLong ) 
    }
    val P2P = sc.textFile( p2pFileName ).map( parseLine _ ).cache()
    val P2City = sc.textFile( p2cityFileName ).map( parseLine _ ).cache()
    val City2Country = sc.textFile( city2countryFileName ).map( parseLine _ ).cache()
    val Country2Continent = sc.textFile( conuntry2continentFileName ).map( parseLine _ ).cache()

    // merge join predicates and transform item

    // use city as partition key
    //val P = P2P.join(P2City).map( x => (x._2._2, (x._1, x._2._1)) ).cache()   // (cid, (sid, did))
    
    // use country as partition key
    // val P = P2P.join(P2City).map( x => (x._2._2, (x._1, x._2._1)) )     // (cid, (sid, did))
    //            .join( City2Country ).map( x => (x._2._2, x._2._1)  ).cache()   // (cityid, ((sid, did), countryid) ) => (countryid, (sid, did))
                
    // use continent as partition key
    val P = P2P.join(P2City).map( x => (x._2._2, (x._1, x._2._1)) )     // (cid, (sid, did))
                .join( City2Country ).map( x => (x._2._2, x._2._1)  )   // (cityid, ((sid, did), countryid) ) => (countryid, (sid, did))
                .join( Country2Continent ).map( x => (x._2._2, x._2._1)).cache()  // (countryid, ((sid, did), continentid) ) => (continentid, (sid, did))
    
    require( P.count() == P2P.count() ) // assert every join is valid ; assumption of REF partitioning
    P2P.unpersist();

    // set partition function and cache
    // since one person is located in one city/country/continent, item with same sid relies on same partition
    
    // use hash partitioner
    //val partFunc = new HashPartitioner(partitionSize)

    // use equisizedpartitioner
    val keyOccurences = P.map( (tup) => (tup._1, 1) ).reduceByKey( _ + _ ).collect()
    val sortedKeyOccurencesWithWeights:List[(Long, Int)] = keyOccurences.sortBy( - _._2).toList // sort by descending order
    // println(s"sortedKeyOccurencesWithWeights=${sortedKeyOccurencesWithWeights}")
    val partFunc = new EquiSizePartitioner[Long](partitionSize, sortedKeyOccurencesWithWeights)

    // repartition
    val partEdges = P.partitionBy( partFunc ).cache()
    if ( partEdges.getNumPartitions != partitionSize ) {
      throw new Exception("Wrong # partitions")
    }

    val sizeDistRdd = partEdges.mapPartitions(iter => Array(iter.size).iterator, true)
    val sizeDist = sizeDistRdd.collect()
    val minPartSize = sizeDist.reduce( _ min _)
    val maxPartSize = sizeDist.reduce( _ max _)

    // foreach partition, accumulate list of all edges
    // foreachPartition is used for order preserving. Also distinct operator does not break ordering
    val edgesSizeAccum = sc.collectionAccumulator[(Int, Int)]("edgesSizeAccum")
    val sidListAccum = sc.collectionAccumulator[(Int, List[Long])]("sidAccum")
    val didListAccum = sc.collectionAccumulator[(Int, List[Long])]("didAccum")
    partEdges.foreachPartition( x => {
      val pid = TaskContext.getPartitionId;
      sidListAccum.add( (pid, x.map( y => y._2._1 ).toList.distinct) )
    } )
    partEdges.foreachPartition( x => {
      val pid = TaskContext.getPartitionId;
      didListAccum.add( (pid, x.map( y => y._2._2 ).toList.distinct) )
    })
    partEdges.foreachPartition( x => {
      val pid = TaskContext.getPartitionId;
      edgesSizeAccum.add( (pid, x.size ) )
    })
    var sidList:List[Long] = Nil
    var didList:List[Long] = Nil
    sidListAccum.value.forEach( sidList ++= _._2 )
    didListAccum.value.forEach( didList ++= _._2 )
    require( sidListAccum.value.size == partitionSize )
    require( didListAccum.value.size == partitionSize )
    // construct sidListAccumOrderingMap ( input : logical pid ; output : actual pid )
    val sidListAccumOrderingMap:List[Int] = sidListAccum.value.asScala.toList.map( _._1 ).zipWithIndex.sortBy( _._1 ).map( _._2 )
    // construc edgeSizeAccumOrderingMap
    val edgeSizeAccumOrderingMap: List[Int] = edgesSizeAccum.value.asScala.toList.map( _._1 ).zipWithIndex.sortBy( _._1 ).map( _._2 )
    

    // distribute edges without outgoing edges to each partition
    val verticesWithoutOutgoingEdges = ( didList.toSet diff sidList.toSet ).toList
    val vertWithoutOutgoingEdgeDistSize = ( ( ( verticesWithoutOutgoingEdges.size - 1 ) / partitionSize ) + 1 )

    // collect and generate verticesWithoutOutgoingEdgesParts
    val verticesWithoutOutgoingEdgesParts:List[List[Long]] =
          verticesWithoutOutgoingEdges.sliding(vertWithoutOutgoingEdgeDistSize, vertWithoutOutgoingEdgeDistSize).toList
    require( verticesWithoutOutgoingEdgesParts.size == partitionSize )

    // retain ordered list of vertices
    val srcVerticesCntPerPartition:List[Int] = Range(0, partitionSize).toList.map( idx => sidListAccum.value.get( sidListAccumOrderingMap(idx) )._2.size )
    val verticesWithoutOutgoingCntPerPartition:List[Int] = Range(0, partitionSize).toList.map( idx => verticesWithoutOutgoingEdgesParts( idx ).size )
    val verticesParts:List[List[Long]] = Range(0, partitionSize).toList.map { idx => {
      List.concat( sidListAccum.value.get( sidListAccumOrderingMap(idx) )._2, verticesWithoutOutgoingEdgesParts( idx ) )
    } }

    // add virtual vertices to keep turbograph's constraint :
    //  1. vid ranges should have same size
    //  2. the start of vid range should be multiples of partitionSize
    val maxSizeOfVertices = verticesParts.map( _.size ).reduce(_ max _)
    val targetVertRangeSize:Int = ( ( ( maxSizeOfVertices - 1 ) / partitionSize ) + 1 ) * partitionSize
    require( targetVertRangeSize % partitionSize == 0 )
    println(s"TargetVerticesRangeSize=${ targetVertRangeSize }")
    // insert virtual edges
    var virtVidCnt = Long.MinValue // use negative long values so that they do not overlap with actual(positive) vids
    val orderedVerticesList:List[Long] = verticesParts.flatMap( part => {
      part ++ Range(0, targetVertRangeSize - part.size).map( _ => { virtVidCnt+=1; virtVidCnt } )
    })
    val vidPaddingCntPerPartition: List[Int] = verticesParts.map( part => targetVertRangeSize - part.size )
    require( orderedVerticesList.size % targetVertRangeSize == 0 )

    val vidRanges: List[(Long, Long)] = (0 to partitionSize).map( pid => (pid*targetVertRangeSize.toLong, (pid+1)*targetVertRangeSize.toLong) ).toList
    val broadcastedVidRanges = sc.broadcast( vidRanges )
    def getPartId(vid: Long):Int = {
      val vr: List[(Long, Long)] = broadcastedVidRanges.value
      vr.indexWhere( range => vid >= range._1 && vid < range._2 );
    }

    // retain ordered list of vertices.
    val verticesCnt = orderedVerticesList.size
    var vidCnt:Long = 0
    val vidMap = MutMap[Long,Long]()
    orderedVerticesList.foreach( v => {
      vidMap += (v -> vidCnt) ;
      vidCnt += 1
    } )
    require( verticesCnt == vidMap.size )

    // broadcast map
    val broadcastedMap = sc.broadcast( vidMap.toMap )

    // perform renumbering
    val renumberedPartEdges = partEdges.map( x => {
      val sid = x._2._1;
      val did = x._2._2;
      ( broadcastedMap.value(sid), broadcastedMap.value(did) )
    } )

    // calculate hit ratio for each partition
    // TODO hit ratio is only valid for self join
    implicit def bool2int(b:Boolean) = if (b) 1 else 0
    val hitCntAccum = sc.collectionAccumulator[(Int, Int)]("hitAccum")
    renumberedPartEdges.foreachPartition( iter => {
      val pid:Int = TaskContext.getPartitionId;
      var hit:Int = 0;
      iter.foreach( tup => hit += bool2int (getPartId(tup._2) == pid) );
      hitCntAccum.add( (pid, hit) );
    })
    val hitCnts:List[Int] = (0 until partitionSize).map( pid => {
      val apid = hitCntAccum.value.asScala.toList.indexWhere( _._1 == pid );
      hitCntAccum.value.get(apid)._2
    }).toList
    val edgeSizes:List[Int] = (0 until partitionSize).map( pid => {
      val apid = edgeSizeAccumOrderingMap(pid);
      edgesSizeAccum.value.get(apid)._2
    }).toList
    val hitRatios:List[Float] = (0 until partitionSize).map( id => hitCnts(id).toFloat / edgeSizes(id).toFloat ).toList

    // sort target vid in each partition
    val renumberedEdgesDF = renumberedPartEdges.toDF( "sid", "tid" )
    val sortedRDD = renumberedEdgesDF.sortWithinPartitions( $"sid".asc, $"tid".asc ).rdd
    // save partitions
    val edgesToStr = sortedRDD.map( row => s"${ row(0) }	${ row(1) }" ) // "v\tv"
    edgesToStr.saveAsTextFile( outputPath )


    /* 
        PRINT STATISTICS
    */
    // println(s"sidListAccumOrderingMap=${sidListAccumOrderingMap}")
    // println(s"edgeSizeAccumOrderingMap=${edgeSizeAccumOrderingMap}")
    // print vidmap info
    println(s"SizeOfTotalVertices=${verticesCnt}")
    // max/min partitionsize
    println(s"maxPartitionedSize=${ maxPartSize }")
    println(s"minPartitionedSize=${ minPartSize }")
    // print statistics
    println(s"srcVerticesCntPerPartition=${srcVerticesCntPerPartition}")
    println(s"verticesWithoutOutgoingCntPerPartition=${verticesWithoutOutgoingCntPerPartition}")
    println(s"vidPaddingCntPerPartition=${vidPaddingCntPerPartition}")
    // hit ratio
    println(s"hitRatioPerPartition=${hitRatios}")

    // max/min vertex id
    val maxVertexIdSrcTarget = renumberedPartEdges.reduce( (a,b) => ( max( a._1, b._1 ) , max(a._2, b._2) )) 
    val minVertexIdSrcTarget = renumberedPartEdges.reduce( (a,b) => ( min( a._1, b._1 ) , min(a._2, b._2) )) 
    println( s"maxSrcVertexId=${ maxVertexIdSrcTarget._1.toString }")
    println( s"maxDstVertexId=${ maxVertexIdSrcTarget._2.toString }")
    println( s"minSrcVertexId=${ minVertexIdSrcTarget._1.toString }")
    println( s"minDstVertexId=${ minVertexIdSrcTarget._2.toString }")
    
    // stop spark context
    sc.stop()
    
  }
}