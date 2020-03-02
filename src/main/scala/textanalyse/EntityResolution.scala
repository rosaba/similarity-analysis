package textanalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class EntityResolution (sc:SparkContext, dat1:String, dat2:String, stopwordsFile:String, goldStandardFile:String) {

  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {

    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */
    val stopW = this.stopWords
    data.map(x => (x._1, EntityResolution.tokenize(x._2, stopW)))
  }

  def countTokens(data: RDD[(String, List[String])]): Long = {

    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */
    data.flatMap(x => x._2.map(x => 1)).count()

  }

  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {

    /*
     * Findet den Datensatz mit den meisten Tokens
     */

    val maxListSize = data.map(_._2.size).max
    val largestLists = data.filter(_._2.size == maxListSize)
    largestLists.first()
  }

  def createCorpus(): Unit = {

    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD), vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     */

    // jeweils noch cachen!
    amazonTokens = getTokens(amazonRDD)
    googleTokens = getTokens(googleRDD)
    corpusRDD = amazonTokens.union(googleTokens)
  }


  def calculateIDF(): Unit = {

    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */

    val numProd: Double = corpusRDD.count // number of products (documents)
    val uniqueTokens = corpusRDD.flatMap(x => x._2.toSet)  // direkt nur values nehmen statt x._2!!
    val idfPerToken = uniqueTokens.map((_, 1)).reduceByKey(_ + _).map(x => (x._1, numProd / x._2))
    idfDict = idfPerToken.collect().toMap

  }


  def simpleSimilarityCalculation: RDD[(String, String, Double)] = {

    /*
     * Berechnung der Document-Similarity für alle möglichen 
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln, bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    val cartesianSmall = amazonRDD.cartesian(googleRDD)

    val idfDict_ = this.idfDict
    val stopWords_ = this.stopWords
    similarities = cartesianSmall.map(x => EntityResolution.computeSimilarity((x._1, x._2), idfDict_, stopWords_))
    similarities
  }


  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {

    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */

    sim.filter(x => x._1 == vendorID1 && x._2 == vendorID2).map(x => x._3).first()
  }

  def simpleSimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    val cartesianSmall = amazonRDD.cartesian(googleRDD)

    val idfDict_ = sc.broadcast(this.idfDict)
    val stopWords_ = this.stopWords
    similarities = cartesianSmall.map(x => EntityResolution.computeSimilarityWithBroadcast((x._1, x._2), idfDict_, stopWords_))
    similarities
  }

  /*
  * 
  * 	Gold Standard Evaluation
  */

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    /*
     * Berechnen Sie die folgenden Kennzahlen:
     * 
     * Anzahl der Duplikate im Sample
     * Durchschnittliche Consinus Similaritaet der Duplikate
     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
     * 
     * 
     * Ergebnis-Tripel:
     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
     */

    val sims = simpleSimilarityCalculationWithBroadcast
    val similar = sims.map(x => (x._1 + " " + x._2, x._3))

    val duplicates = similar.join(goldStandard)
    val noDuplicates = similar.subtractByKey(duplicates) // chachen!
    val anzDuplikate = duplicates.count()

    val avgCosDup = duplicates.map(_._2._1).sum() / anzDuplikate
    val avgCosNotDup = noDuplicates.map(_._2).sum() / noDuplicates.count()

    (anzDuplikate, avgCosDup, avgCosNotDup)

  }
}

object EntityResolution{
  
  def tokenize(s:String, stopws:Set[String]):List[String]= {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/
     Utils.tokenizeString(s).filter(!stopws.contains(_))
   }

  def getTermFrequencies(tokens:List[String]):Map[String,Double]={
    
    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */

    tokens.groupBy(x => x).mapValues(_.length / tokens.size.toDouble)
  }
   
  def computeSimilarity(record:((String, String),(String, String)), idfDictionary:Map[String,Double], stopWords:Set[String]):(String, String, Double)={
    // KEIN TEST!

    /*
     * Berechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, indem
     * Sie die erforderlichen Parameter extrahieren
     */
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  }
  
  def calculateTF_IDF(terms:List[String], idfDictionary:Map[String,Double]):Map[String,Double]={
    
    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern / Tokens
     * Ergebnis ist eine Map, die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     * (TF-IDF is the term frequency multiplied by the idf value)
     */
    
    val freq = getTermFrequencies(terms)
    freq.map(product => (product._1, product._2 * idfDictionary(product._1)))
  }
  
  def calculateDotProduct(v1:Map[String,Double], v2:Map[String,Double]):Double={
    
    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */

    v1
      .foldLeft(Map.empty[String, Double])( (map, value) =>
        map + (value._1 -> v1(value._1) * v2.getOrElse(value._1, 0.0)) )
      .values
      .sum

  }

  def calculateNorm(vec:Map[String,Double]):Double={
    
    /*
     * Berechnung der Norm eines Vectors (berechnet L2-Norm für einen Vektor)
     */

    math.sqrt(vec.map(product => product._2 * product._2).sum)

  }
  
  def calculateCosinusSimilarity(doc1:Map[String,Double], doc2:Map[String,Double]):Double={
    
    /* 
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */
    
    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2))
  }
  
  def calculateDocumentSimilarity(doc1:String, doc2:String, idfDictionary:Map[String,Double], stopWords:Set[String]):Double={
   
    /*
     * Berechnung der Document-Similarity für ein Dokument
     */

    val d1 = tokenize(doc1, stopWords)
    val d2 = tokenize(doc2, stopWords)

    val d1_tfidf = calculateTF_IDF(d1, idfDictionary)
    val d2_tfidf = calculateTF_IDF(d2, idfDictionary)

    calculateCosinusSimilarity(d1_tfidf, d2_tfidf)
  }
  
  def computeSimilarityWithBroadcast(record:((String, String),(String, String)), idfBroadcast:Broadcast[Map[String,Double]], stopWords:Set[String]):(String, String, Double)={
    
    /*
     * Berechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, indem
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */

     (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfBroadcast.value, stopWords))
  }
}
