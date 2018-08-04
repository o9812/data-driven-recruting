import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.{Pipeline, PipelineModel}
import java.sql.Date
import org.apache.spark.sql.types.{DateType, IntegerType}


/**  load data */
val myUDf = udf((s: String)= > s.split(","))
val df = spark.sqlContext.read.json("./project_BDAD/candidatesBDAD")
val newDF = df.withColumn("splitted", myUDf(df("_corrupt_record")))
val candidatesDF = newDF.withColumn("candidateId", newDF("splitted").getItem(0)).withColumn("email", newDF("splitted").getItem(1)).withColumn("gender", newDF("splitted").getItem(2)).withColumn("isMinority", newDF("splitted").getItem(3)).withColumn("isIvy", newDF("splitted").getItem(4)).withColumn("recruiterId", newDF("splitted").getItem(5)).withColumn("candidateCreationDate", newDF("splitted").getItem(6)).withColumn("zip", newDF("splitted").getItem(7)).withColumn("isWillingRelocate", newDF("splitted").getItem(8)).withColumn("clarity", newDF("splitted").getItem(9)).withColumn("personability", newDF("splitted").getItem(10)).withColumn("yearBeganExperience", newDF("splitted").getItem(11)).withColumn("i9", newDF("splitted").getItem(12)).withColumn("isPermanent", newDF("splitted").getItem(13)).withColumn("isConsulting", newDF("splitted").getItem(14)).withColumn("resume_text", newDF("splitted").getItem(15)).withColumn("mainCategory", newDF("splitted").getItem(16)).withColumn("funcCategory", newDF("splitted").getItem(17)).withColumn("techCategory", newDF("splitted").getItem(18)).withColumn("writeup", newDF("splitted").getItem(19)).drop("_corrupt_record").drop("splitted")


/** select the columns and filter out null */
val candidatesDF_resume_space_null = candidatesDF.select("candidateCreationDate", "resume_text").filter(col("resume_text").notEqual("")).filter(col("resume_text").notEqual("null"))

/**  clean html parse */
val df_html = candidatesDF_resume_space_null.withColumn("resume_text_html", regexp_replace(candidatesDF_resume_space_null("resume_text"), "<.+?>", "")).filter(col("resume_text_html").notEqual(""))

/** remove punctuation */
val df_punctuation = df_html.withColumn("resume_text_cleaned", regexp_replace(df_html("resume_text_html"), """(&#9679|&nbsp|&bull|&#61623|gmail.com|[\p{Punct}&&[^.$]]|8203|159|\b\p{IsLetter}{1,2}\b)\s*""", " ")).filter(col("resume_text_cleaned").notEqual(""))


/**  regexTokenizer */
val regexTokenizer = new RegexTokenizer().setInputCol("resume_text_cleaned").setOutputCol("resume_text_token").setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

/**  remove stop words */
val remover = new StopWordsRemover().setInputCol("resume_text_token").setOutputCol("resume_text_token_filtered")

/**  set 2 gram */
val ngram = new NGram().setN(2).setInputCol("resume_text_token_filtered").setOutputCol("resume_text_ngrams")

/** pipline */
val pipeline = new Pipeline().setStages(Array(regexTokenizer, remover, ngram))

/** transform from ML lib */
val ngramDataFrame = pipeline.fit(df_punctuation).transform(df_punctuation)



/** concat all words as a corp ,join with "_" */
val df_resume_text_cleaned = ngramDataFrame.withColumn("concatenate_list", concat_ws("_", $"resume_text_ngrams"))

/** work with date make string to date type */
val df_resume_text_cleaned1 = df_resume_text_cleaned.withColumn("candidateCreationDate_date", col("candidateCreationDate").cast("date"))

val cleaned_df = df_resume_text_cleaned1.select("candidateCreationDate_date", "concatenate_list")
/** save the dataframe
cleaned_df.write.save("project_BDAD/output/cleaned_df")
/** load the dataframe */
val df_resume_text_cleaned1 = spark.read.load("project_BDAD/output/cleaned_df")*/


/** filter by date create rdd and do the word count */
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2017-01-01", "2017-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2017")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2016-01-01", "2016-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2016")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2015-01-01", "2015-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2015")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2014-01-01", "2014-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2014")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2013-01-01", "2013-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2013")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2012-01-01", "2012-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2012")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2011-01-01", "2011-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2011")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2010-01-01", "2010-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2010")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2009-01-01", "2009-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2009")*/

val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2008-01-01", "2008-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2008")*/

val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
/** wrd_cnt_sort.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2007")*/
