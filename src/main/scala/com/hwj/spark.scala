package com.hwj

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object spark {

  /**
    * 统计关系出现的次数
    * @param sc
    * @param path：边文件
    * @param num：关系数量阈值
    * @return
    */
  def edgeCount(sc:SparkContext,path:String,num:Int) ={
    val textFile = sc.textFile(path)
    val counts = textFile.map(word => (word, 1))
      .reduceByKey(_ + _).filter(_._2>num)
//    counts.collect().foreach(println)
    counts
  }

  /**
    * 构建图
    * @param sc
    * @param path1:顶点文件
    * @param path2：边文件
    * @param num：关系数量阈值
    */
  def creatGraph(sc:SparkContext,path1:String,path2:String,num:Int) ={
    val hero = sc.textFile(path1)
    val counts = edgeCount(sc,path2,num)

    val verticesAll = hero.map { line =>
      val fields = line.split(' ')
      (fields(0).toLong, fields(1))
    }

    val edges = counts.map { line =>
      val fields = line._1.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, line._2)//起始点ID必须为Long，最后一个是属性，可以为任意类型
    }
    val graph_tmp = Graph.fromEdges(edges,1L)
//    经过过滤后有些顶点是没有边，所以采用leftOuterJoin将这部分顶点去除
    val vertices = graph_tmp.vertices.leftOuterJoin(verticesAll).map(x=>(x._1,x._2._2.getOrElse("")))
    val graph = Graph(vertices,edges)

    graph
  }

  /**
    * 输出为gexf格式
    * @param g：图
    * @tparam VD
    * @tparam ED
    * @return
    */
  def toGexf[VD,ED](g:Graph[VD,ED]) ={
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n  " +
      "<nodes>\n " +
      g.vertices.map(v => "  <node id=\""+v._1+"\" label=\""+v._2+"\" />\n").collect().mkString+
      "</nodes>\n  "+
      "<edges>\n"+
      g.edges.map(e => "  <edge source=\""+e.srcId+"\" target=\""+e.dstId+"\" weight=\""+e.attr+"\"/>\n").
        collect().mkString+
      "</edges>\n        </graph>\n      </gexf>"

  }

  /**
    * 找出度为1或2的点
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def minDegrees[VD,ED](g:GraphOps[VD,ED])={
//    g.degrees.filter(_._2<3).map(_._1).collect().mkString("\n")
      g.degrees.filter(_._2<3).map(_._1).collect().map(a =>a.toInt)
  }

  /**
    * 使用连通组件找到孤岛人群
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def isolate[VD,ED](g:GraphOps[VD,ED]) ={
    g.connectedComponents.vertices.map(_.swap).groupByKey().map(_._2).collect().mkString("\n")
  }

  /**
    * 合并2张图
    * @param g1
    * @param g2
    * @return
    */
  def mergeGraphs(g1:Graph[String,Int],g2:Graph[String,Int]) ={
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct().zipWithIndex()

    def edgeWithNewVid(g:Graph[String,Int]) ={
      g.triplets.map(et=>(et.srcAttr,(et.attr,et.dstAttr)))
        .join(v)
        .map(x => (x._2._1._2,(x._2._2,x._2._1._1)))
        .join(v)
        .map(x=> new Edge(x._2._1._1,x._2._2,x._2._1._2))
    }
    def reduceEdge(g3:Graph[String,Int],g4:Graph[String,Int])={
      edgeWithNewVid(g3).union(edgeWithNewVid(g4)).
        map(e=>((e.dstId,e.srcId),e.attr)).
        reduceByKey(_+_).
        map(e=>Edge(e._1._1,e._1._2,e._2))
    }
    Graph(v.map(_.swap),reduceEdge(g1,g2))
  }

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("spark").setMaster("local")
    val sc = new SparkContext(conf)

//    val folderPath ="C:\\Users\\hwj\\Documents\\金庸数据\\三部曲（含昵称）\\"
    val folderPath =".\\resources\\人物（含昵称）\\"
//    val folderPath = "hdfs://hadoop000:8020/data/jy/hero"
    val graph1 = creatGraph(sc,folderPath+"result1\\heromap.txt",
      folderPath+"result1\\relationship.txt",15).cache()

    val graph2 = creatGraph(sc,folderPath+"result2\\heromap.txt",
      folderPath+"result2\\relationship.txt",15).cache()

    val graph3 = creatGraph(sc,folderPath+"result3\\heromap.txt",
      folderPath+"result3\\relationship.txt",15).cache()

    val graph4 = mergeGraphs(graph1,graph2)
    val graphAll = mergeGraphs(graph3,graph4).cache()

    val minDegreeArray = minDegrees(graphAll)
//    val graphMin = graphAll.subgraph(vpred = (id,attr)=>minDegreeArray.contains(id),epred = e=>minDegreeArray.contains(e.srcId) || minDegreeArray.contains(e.dstId))
    val graphMin = graphAll.subgraph(epred = e=>(minDegreeArray.contains(e.srcId) || minDegreeArray.contains(e.dstId)))
//    graphMin.edges.collect().foreach(println)
//    println(graphAll.edges.collect().length)
//    graphAll.edges.map(e=>((e.dstId,e.srcId),e.attr)).reduceByKey(_+_).map(e=>Edge(e._1._1,e._1._2,e._2)).foreach(println)




//    val folderPath2 ="C:\\Users\\hwj\\Documents\\金庸数据\\人物与武功秘籍\\"
    val folderPath2 =".\\resources\\人物与武功秘籍\\"
//    val folderPath2 = "hdfs://hadoop000:8020/data/jy/weapon"
    val graph11 = creatGraph(sc,folderPath2+"result1\\hero.txt",
      folderPath2+"result1\\hero_weapon.txt",8).cache()

    val graph12 = creatGraph(sc,folderPath2+"result2\\hero.txt",
      folderPath2+"result2\\hero_weapon.txt",8).cache()

    val graph13 = creatGraph(sc,folderPath2+"result3\\hero.txt",
      folderPath2+"result3\\hero_weapon.txt",8).cache()
    val graph14 = mergeGraphs(graph11,graph12)
    val graphWeapon = mergeGraphs(graph13,graph14).cache()

    // 输出到文件
    val outputPath =".\\output\\"
    val pw1 = new PrintWriter(outputPath+"graph.gexf")
    pw1.write(toGexf(graphAll))
    val pw2 = new PrintWriter(outputPath+"isolate.txt")
    pw2.write(isolate(graphAll))
    val pw3 = new PrintWriter(outputPath+"minDegrees.gexf")
    pw3.write(toGexf(graphMin))
    val pw4 = new PrintWriter(outputPath+"graphWeapon.gexf")
    pw4.write(toGexf(graphWeapon))


    pw1.close()
    pw2.close()
    pw3.close()
    pw4.close()
    sc.stop()



  }


}
