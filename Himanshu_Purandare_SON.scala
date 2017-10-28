import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark.rdd.RDD
import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.RangePartitioner
import scala.util.control.Breaks._

object Himanshu_Purandare_SON {
	def main(args: Array[String]) {
    var txtFileUsers = "users.dat"
    var txtFileRatings = "ratings.dat"
    var caseToRun = 2
    var support:Int = 600
    var FinalOut = List[List[Int]]()
    if (args.length != 4) {
      txtFileUsers = "users.dat"
      txtFileRatings = "ratings.dat"
        caseToRun = 1
        support = 1200
    }
    else {
      caseToRun = args(0).toInt
      txtFileUsers = args(1)
      txtFileRatings = args(2)
      support = args(3).toInt
    }
    
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val txtFileLinesUsers = sc.textFile(txtFileUsers , 4).cache()
    val txtFileLinesRatings = sc.textFile(txtFileRatings , 4).cache()
    val Ratings = txtFileLinesRatings.flatMap(x => createDataMapForRatings(x))
    
    if (caseToRun == 1) {
      
    val Users = txtFileLinesUsers.flatMap(x => createDataMapForMaleUsers(x))
    
    val RatingsJoinedUsers = Ratings.join(Users)
    val UsersRatings = RatingsJoinedUsers.flatMap(x => createDataMapForUserRatings(x._1, x._2 ._1))
	val UsersRatingsGroup = UsersRatings.aggregateByKey(scala.collection.mutable.Set.empty[Int])((numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1++=numList2; numList1}).mapValues(_.toSet).cache()
	UsersRatingsGroup.persist()
	val mapped = UsersRatingsGroup.mapPartitions(iterator => 
	  apriori_algo(iterator, support.toFloat/UsersRatingsGroup.getNumPartitions).iterator)
    var mappp = mapped.reduceByKey(_+_)
    var dd = mappp.keys.distinct.collect
    
    val Map2 = UsersRatingsGroup.mapPartitions(it => 
      checkReallyTrue(dd, it).iterator)
    var Reduce2 = Map2.reduceByKey(_+_)
    
    for (kk <- Reduce2.collect){
      if (kk._2 >= support) {
        FinalOut = kk._1.toList.sortWith(_<_) :: FinalOut
      }
    }
    //println(FinalOut.size)
    //FinalOut.sortBy(t=>t.sum).sortBy(a=>a(0)).sortBy(s=>s.size).foreach(f=>println("("+f.mkString(",")+")"))
	//FinalOut.sortBy(_._._root_).sortBy(s=>s.size).foreach(f=>println("("+f.mkString(",")+")"))
    //FinalOut.sortBy(a=>a(0)).sortBy(s=>s.size).foreach(f=>println("("+f.mkString(",")+")"))
    }
    else if(caseToRun ==2) {
      val FeUsers = txtFileLinesUsers.flatMap(x => createDataMapForFeMaleUsers(x))
    val RatingsJoinedFeUsers = Ratings.join(FeUsers)
	val FeUsersRatings = RatingsJoinedFeUsers.flatMap(x => createDataMapForFeUserRatings(x._2 ._1, x._1))
    //val FeUsersRatingsGroup = FeUsersRatings.groupByKey.mapValues(_.toSet[Int]).cache()
	val FeUsersRatingsGroup = FeUsersRatings.aggregateByKey(scala.collection.mutable.Set.empty[Int])((numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1++=numList2; numList1}).mapValues(_.toSet).cache()
    FeUsersRatingsGroup.persist()
    val mapped = FeUsersRatingsGroup.mapPartitions(iterator => 
	  apriori_algo(iterator, support.toFloat/FeUsersRatingsGroup.getNumPartitions).iterator)
    
    var mappp = mapped.reduceByKey(_+_)
    var dd = mappp.keys.distinct.collect
    
    val Map2 = FeUsersRatingsGroup.mapPartitions(it => 
      checkReallyTrue(dd, it).iterator)
    var Reduce2 = Map2.reduceByKey(_+_)
    
    for (kk <- Reduce2.collect){
      if (kk._2 >= support) {
        FinalOut = kk._1.toList.sortWith(_<_) :: FinalOut
      }
    }
    //println(FinalOut.size)
    
    }
    //var FinalToPrint = FinalOut.sortBy(a=>a(0)).sortBy(f=>f.size)
    //var finalListToPrint = List[List[String]]()
    /*
    var c = 1
    var finalListToPrint = scala.collection.mutable.HashMap[Int,List[List[Int]]]()
    for (i<-FinalToPrint.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
      if (i.size==c){
          //var temp_str = ("("+i.mkString(",")+")")
          var name = finalListToPrint.getOrElse(c,List[List[Int]]())
          name = i :: name
          finalListToPrint.update(c, name)
      }
      else if(i.size==c+1){	        
           //var temp_str ="("+i.mkString(",")+")"
           var name = finalListToPrint.getOrElse(c+1,List[List[Int]]()) 
          name = i :: name
          finalListToPrint.update(c+1, name)
          c+=1
          }
      }
      
    var FIN = scala.collection.mutable.HashMap[Int,List[List[Int]]]() 
    for (dd <- finalListToPrint.keys) {
      var temp = List[List[Int]]()
      temp = finalListToPrint(dd)
      FIN.update(dd, temp.sortWith(getSortedList))
    }
    
         
        FIN.foreach(f=>println(f))
        * 
        */
    
    var c = 1
    var fin_list = scala.collection.mutable.HashMap[Int,List[String]]()
    for (i<-FinalOut.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
      if (i.size==c){
          //var temp_str = ("("+i.mkString(",")+")")
        var temp_str = ""
          for (x<-i){
           for (y <-0 to (5-x.toString.length())){
             temp_str += "0"  
           }
           temp_str += x.toString +","
          }
          temp_str = temp_str.dropRight(1)
          var name = fin_list.getOrElse(c,List[String]())
          name = temp_str :: name
          fin_list.update(c, name)
      }
      else if(i.size==c+1){	        
           var temp_str = ""
          for (x<-i){
           for (y <-0 to (5-x.toString.length())){
             temp_str += "0"  
           }
           temp_str += x.toString +","
          }
          temp_str = temp_str.dropRight(1)
           var name = fin_list.getOrElse(c+1,List[String]()) 
          name = temp_str :: name
          fin_list.update(c+1, name)
          c+=1}}
         
    var FIN = scala.collection.mutable.HashMap[Int,List[String]]() 
    for (dd <- fin_list.keys) {
      var temp = List[String]()
      temp = fin_list(dd)
      FIN.update(dd, temp.sortWith(getSorted))
    }
         
        //FIN.foreach(f=>println(f))
        
    var StringToPrint = ""
      for (cc <- FIN(1)) {
        StringToPrint += "("+cc.toInt+"), "
      }
        StringToPrint = StringToPrint.dropRight(2) + "\n"
    for (cc<-2 to FIN.size){
      for (dd <- FIN(cc)) {
        var SplitOnComma = dd.split(",")
        var teStr = "("
        for (it <- SplitOnComma) {
          teStr += it.toInt + ","
        }
        StringToPrint += teStr.dropRight(1) + "), "
      }
      StringToPrint = StringToPrint.dropRight(2)+"\n"
    }
    //print(StringToPrint)
    
    var FileName = "Himanshu_Purandare_SON.case"+caseToRun+"_"+support+".txt"
    val pw = new PrintWriter(new File(FileName))
	pw.write(StringToPrint)
	pw.close
    
    //print("FinalList: \n\n"+finalListToPrint)
	}
	/*
	def get_sum(l:List[Int]):Int = {
	  var sum = 0
	  for (ii <- l) {
	    sum += ii
	  }
	  return sum
	}
	
	def getSortedList(x:List[Int],y:List[Int]) = {
	  x<y
	  }*/
	
	def getSorted(s:String, c:String) = {s<c}
	
	def createDataMapForFeUserRatings(MovieId:Int, UserId:Int): Map[Int, Int] = {
	 
	 val dataMap = Map[Int, Int]((MovieId  -> UserId))
	 
	return dataMap
	}
	
	def checkReallyTrue(dd:Array[Set[Int]], iter: Iterator[(Int, scala.collection.immutable.Set[Int])]):HashMap[Set[Int],Int] = {
		var outp=HashMap[Set[Int],Int]()

		for (ii <- iter) {
		  for (jj <- dd) {
		    if (ii._2 .intersect(jj).equals(jj)) {
		    outp += jj -> (outp.getOrElse(jj, 0) + 1)
		    }
		  }
		}
		return outp
	}
	
	def createDataMapForMaleUsers(data:String): Map[Int, String] = {
	 val array = data.split("::")
	 var dataMap = Map[Int, String]()
	 if (array(1) == "M") {
	 dataMap = Map((array(0).toInt -> array(1)))
	 }
	return dataMap
	}
	
	def createDataMapForFeMaleUsers(data:String): Map[Int, String] = {
	 val array = data.split("::")
	 var dataMap = Map[Int, String]()
	 if (array(1) == "F") {
	 dataMap = Map((array(0).toInt -> array(1)))
	 }
	return dataMap
	}
	
	def createDataMapForRatings(data:String): Map[Int, Int] = {
	 val array = data.split("::")
	 val dataMap = Map[Int, Int]((array(0).toInt -> array(1).toInt))
	return dataMap
	}
	
	def createDataMapForUserRatings(UserId:Int, MovieId:Int): Map[Int, Int] = {
	 
	 val dataMap = Map[Int, Int]((UserId -> MovieId))
	 
	return dataMap
	}
	
	
	
	def apriori_algo(UsersRatingsGroup: Iterator[(Int, scala.collection.immutable.Set[Int])], support:Float): Map[Set[Int],Int] = {
	  var FrequentItem = new ListBuffer[Set[Int]]()
	  var values = List[Set[Int]]()
	  for (pp <- UsersRatingsGroup ) {
	    values = pp._2 :: values
	  }
	  var Cand= HashMap[Set[Int], Int]()
	  var FinalFrequent = new ListBuffer[Set[Int]]()
	  for (i <- values) {
	    for (ii <- i) {	      
	      var x = Cand.getOrElse(Set(ii), 0) + 1
	      Cand += Set(ii) -> x
	    }
	  }
	  
	  for (ii <- Cand) {
	    if (ii._2 >= support){ 
	      FrequentItem += ii._1 
	    }
	  }
	  
	  FinalFrequent ++= FrequentItem
	  
	  var Len1Freq = Set[Int]()
	  for (kk <- FrequentItem){
	    Len1Freq = Len1Freq.union(kk)
	  }
	  var countLen = 2
	  while (!FrequentItem.isEmpty) {
	    var tempCand = new ListBuffer[Set[Int]]()
	    var visited = Set[Set[Int]]()
	    Cand = Cand.empty
	    //var countLen = 0
	    //countLen = (FrequentItem(0).size + 1)
	    for (i <- 0 until FrequentItem.size) {
	      var diffList = Len1Freq.diff(FrequentItem(i)).toList
	      for (j <- 0 until diffList.toList.size) {
	        var temp = FrequentItem(i).union(Set(diffList(j)))
	        //println("temp"+countLen+temp)
	        if (!(visited.contains(temp))) {
	          visited = visited.union(Set(temp))
	          var checkCand = temp.toList.combinations(countLen-1).toList
	          var CheckFlag = true
	          for (jj<-checkCand) {
	        	  if (! (FrequentItem.contains(jj.toSet))) {
	        		  CheckFlag = false
	        	  }
	          }
	          if (CheckFlag) {
	        	  for (iii <- values) {
	        		  if (iii.intersect(temp).equals(temp)) {
	        			  Cand += temp -> (Cand.getOrElse(temp, 0) + 1)
	        		  }
	        	  }
	          }
	        }
	      }
	    }
	    
	  FrequentItem = new ListBuffer[Set[Int]]()
	  for (ii <- Cand) {
	    if (ii._2 >= support){ 
	      FrequentItem += ii._1 
	    }
	  }
	  FinalFrequent ++= FrequentItem
	  countLen += 1
	  }
	  var FFrequent = Map[Set[Int],Int]() 
	  FinalFrequent.foreach(f=>FFrequent += f->1)
	  return FFrequent
	}
}
