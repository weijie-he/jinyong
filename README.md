# 使用Spark Graphx 探索你不知道的金庸武侠世界

GitHub地址：https://github.com/weijie-he/jinyong

### 一、缘起

2018年10月30日，金庸在香港逝世，享年94岁。

知道这个消息之后，我的情绪很低落，讲台上老师在讲什么仿佛也听不见了，脑海中一直在回想着先生写过的关于离别的句子。

> 程英道：“三妹，你瞧这些白云聚了又散，散了又聚，人生离合，亦复如斯。你又何必烦恼？” 她话虽如此说，却也忍不住流下泪来。

> 却听得杨过朗声说道：“今番良晤，豪兴不浅，他日江湖相逢，再当杯酒言欢。咱们就此别过。”

金庸先生告诉了我什么是“侠”。作为先生的忠实读者，我觉得自己该做点什么来缅怀先生，以我自己的方式。

正好，我在学Spark，便想到了利用Spark Graphx 做金庸小说人物关系分析图。

### 二、需求分析

金庸先生给我们留下了什么呢？最著名的无非是“飞雪连天射白鹿，笑书神侠倚碧鸳”这14本小说了。最容易想到的便是对这14本书做一张人物关系分析图。但这一来人物太多，最后画出的图会很大；二来不同书之间的人物很多也没什么关联，硬把他们放在同一张图里并不妥当。最终我决定只选取人物联系最紧密的“射雕三部曲”（《射雕英雄传》、《神雕侠侣》、《倚天屠龙记》）来进行分析。

但是只分析**人物**又感觉略显单薄。金庸小说中还有一些其他的元素，比如如雷贯耳的**称号**（东邪西毒）、高深莫测的**武功**（黯然销魂掌）、**神兵利器**（倚天剑、屠龙刀）。我想把这些元素也加入到分析之中。

同时还要考虑怎么利用Spark Graphx 的图计算功能，做一些有意义的分析。

最终确立了以下需求：

1. 分析人物之间的亲密度关系

2. 找出“专属昵称”

     （很多人物之间的交流并不会直呼其名，比如黄蓉会叫郭靖“靖哥哥”，我想找出类似的“专属昵称”）

3. 探索小说人物中“孤岛群体”（即“小圈子”）

4. 有没有谁经常被某种武功/兵器揍 

###  三、工作流程

#### 3.1 获取"射雕三部曲"小说原文、人物名册、称号\武功\武器大全等初始数据

   ​	小说原文很容易获取，人物名册、称号\武功\武器大全 等也可以在网上搜到。

#### 3.2 数据预处理，将数据转换成Graphx需要的格式

   ​	Graphx需要的是顶点集和边集的信息。

   ​	在人物亲密度图中，我将人名、昵称作为顶点；在人物—武器关系图中，我将人名、武器、武功作为顶点。

   至于边集信息，是这样确定的：以原文中每一句话为单位。如果在这句话中，出现了两个上述的“顶点”，则认为他们产生了一次联系。如果在这句话中，出现了三个“顶点”，则认为他们两两之间都有一次联系。以此类推。

​	处理完的结果保存在resources文件夹中。结果如下所示
<div style="float:left;margin-right:20px" >
	<img src="https://ws1.sinaimg.cn/large/50c1811fly1g14lnzvsu2j205r0e5gly.jpg" width="100px" height="300px" >
</div>
<div style="float:left" >
	<img src="https://ws1.sinaimg.cn/large/50c1811fly1g14lo54gfij204m0bzq2t.jpg" width="100px" height="300px" >
</div>

#### 3.3 使用Spark GraphX 生成图

我想把联系的次数作为边的权重。首先就要统计同一个联系出现的次数。这一步有点像WordCount，由于不想让一些打酱油的人物出现，所以还用了个filter函数过滤。

```scala
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
```

使用顶点集和边集构建图

```scala
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
```

至此，需求中的第一点：人物亲密度关系图已经生成。

类似的，我们更换一下顶点集和边集，就可以生成人物——武器\武功的关系图，从而找出有没有谁经常被某种武功/兵器揍。

#### 3.4 使用Spark GraphX 处理图

可以通过找出度为1或2的点，来寻找“专属昵称”。

```scala
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
```

通过使用内置函数connectedComponents()可以找到小说人物中“孤岛群体”（即“小圈子”）。

```scala
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
```

由于之前我们是每本书都生成一张图，最后我们还需要把这几张图合并为一张图。

思路就是先取得所有顶点信息，去除，再对这些顶点重新编号。再对这些新生成的点重新构建边。

```scala
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
```

#### 3.5 导出到Gephi

我们可以把图像按照gexf格式输出，然后在Gephi中打开，就可以进行图形化展示。

```scala
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
```

### 四、结果展示与分析
以下图片的高清完整版可在output/pics中找到
#### 4.1 人物亲密度关系分析
![人物亲密度关系图](https://ws1.sinaimg.cn/large/50c1811fly1g14npx182gj215w0mcatc.jpg)


可以看出郭靖和黄蓉的颜色是最深的（联系是最紧密的）。这是因为他们在《射雕》和《神雕》中都有很多戏份。《神雕》中的男女主小龙女和杨过联系也很紧密。相比之下《倚天》中的男女主张无忌和赵敏直接的线就淡的多了。一方面，这是因为赵敏的出场时间太晚（全书40章，赵敏在第23章才出场）。另一方面，张无忌优柔寡断，情感方面也一直在赵敏和周芷若之间犹豫不决，导致张无忌的情感线被周芷若分流了许多。

#### 4.2 “专属昵称”分析
<div align="center" >
	<img src="https://ws1.sinaimg.cn/large/50c1811fly1g14o9pr7uzj20j20g116w.jpg" width="450px" height="400px" >
</div>

由于我只是筛选出了度为1和2的点，但有些点是人名，而不是昵称，不必看。

我原来以为“专属昵称”只出现在情侣之间，但发现有两个例外。

- 洪七公——靖儿

  这两人情同父子。郭靖自幼丧父，洪七公也没有子嗣。俗话说，“一日为师终身为父”，我觉得这两个人不是父子，甚是父子。所以有这样的“专属昵称”也不奇怪。

  也许江南七怪也和郭靖情同父子，但可能是因为出现的频率不够高，所以被过滤掉了，这张图上并没有出现。

- 陆无双——傻蛋

  全书只有陆无双一人可以叫杨过”傻蛋“，因为当初杨过骗陆无双自称傻蛋。

> 那道姑**笑**道：“我几时骗过你了？喂，小子，你叫甚么名字？”杨过道：“人人都叫我傻蛋，你不知道么？你叫甚么名字？”那道姑**笑**道：“傻蛋，你只叫我仙姑就得啦。”

​	摘录了一下原文，发现短短几句话，这道姑（陆无双）就笑了2次，足见他们相处的多么愉快。过儿一生孤苦，和陆无双在一起的日子也算是为数不多的快乐时光。我觉得他们俩很有成为情侣的可能，只可惜过儿心里已经有了小龙女。最后他们俩结为了兄妹，也算是一段“有情人终成兄妹”的悲剧故事。

#### 4.3 “孤岛人群”分析
<div align="center" >
	<img src="https://ws1.sinaimg.cn/large/50c1811fgy1g14r9yj0plj20p704379d.jpg" width="90%" height="100px" >
</div>
发现只有3个“孤岛人群”（小团体）。

简捷和薛公远是《倚天屠龙记》中被金花婆婆打伤，找胡青牛治病的人。和他们有交集的人确实很少。

李萍被段天德绑架，很长一段时间内只有他们两个在一起，别人都不知道他们去了哪。

术赤和察合台是成吉思汗的两个儿子。和他们有交集的人也很少。

这三本书中涉及到的人物，即使过滤完，也有将近200号人。如果在现实生活中，200人中应该会有更多的小团体，而且也不会全是2人组，可能有3~5人小团体。

以下是我认为可能的两点原因：

1. 小说中，配角是为主角服务的，一般不会独立于主线人物之外去写小团体

2. 即便需要，写2人也够了，没必要花笔墨写

#### 4.4 人物——武功\兵器分析

![](https://ws1.sinaimg.cn/large/50c1811fgy1g14rqupcvdj21hc0u0b2b.jpg)

主要想看谁经常被哪种武功\兵器揍。

- 无忌——玄冥神掌

  无忌小时候就因为中了玄冥神掌差点死掉，长大后也经常和玄冥二老斗。

- 郭靖——蛤蟆功

  蛤蟆功可以说是郭靖发明的，就是因为他篡改了《九阴真经》，写了本“九阴假经”，才让欧阳锋练成了蛤蟆功。后来也数次和欧阳锋的蛤蟆功交手。《神雕》中小杨过也学了点蛤蟆功，被郭靖发现了，这又产生了一次交集。

- 杨过——金轮、浮尘

  这是书中两大反派金轮法王和李莫愁的武器。

### 五、后记

  人人都知道金庸，可大多是通过影视作品，读过原著的人少的可怜。做这个项目，在缅怀先生的同时，也希望有更多的人能去读一读原著，体会一下先生笔下原汁原味的江湖。
