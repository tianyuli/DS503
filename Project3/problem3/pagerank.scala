val file = spark.read.textFile("soc-LiveJournal1.txt").rdd
var header = file.first()
var lines = file.filter(row => row != header)
val pairs = lines.map{ s =>
      val parts = s.split("\\s+")               // Splits a line into an array of 2 elements according space(s)
             (parts(0), parts(1))               // create the parts<url, url> for each line in the file
    }
val links = pairs.distinct().groupByKey()

var ranks = links.mapValues(v => 1.0)
for (i <- 1 to 50) {
    val contribs = links.join(ranks)         // join  -> RDD1
         .values                           // extract values from RDD1 -> RDD2          
         .flatMap{ case (urls, rank) =>    // RDD2 ->  RDD
                 val size = urls.size        
                     urls.map(url => (url, rank / size))   
             }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.05 + 0.95 * _) // ranks RDD 0.05 is to avoid the url without outlink or inter link
}
var sorted = ranks.sortBy({ case (_, rank) => rank }, ascending = false)
sorted.take(100).foreach(println)

