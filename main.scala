# load data
val myUDf = udf((s: String)= > s.split(","))
val df = spark.sqlContext.read.json("./project_BDAD/candidatesBDAD")
val newDF = df.withColumn("splitted", myUDf(df("_corrupt_record")))

val candidatesDF = newDF.withColumn("candidateId", newDF("splitted").getItem(0)).withColumn("email", newDF("splitted").getItem(1)).withColumn("gender", newDF("splitted").getItem(2)).withColumn("isMinority", newDF("splitted").getItem(3)).withColumn("isIvy", newDF("splitted").getItem(4)).withColumn("recruiterId", newDF("splitted").getItem(5)).withColumn("candidateCreationDate", newDF("splitted").getItem(6)).withColumn("zip", newDF("splitted").getItem(7)).withColumn("isWillingRelocate", newDF("splitted").getItem(8)).withColumn("clarity", newDF("splitted").getItem(9)).withColumn("personability", newDF("splitted").getItem(10)).withColumn("yearBeganExperience", newDF("splitted").getItem(11)).withColumn("i9", newDF("splitted").getItem(12)).withColumn("isPermanent", newDF("splitted").getItem(13)).withColumn("isConsulting", newDF("splitted").getItem(14)).withColumn("resume_text", newDF("splitted").getItem(15)).withColumn("mainCategory", newDF("splitted").getItem(16)).withColumn("funcCategory", newDF("splitted").getItem(17)).withColumn("techCategory", newDF("splitted").getItem(18)).withColumn("writeup", newDF("splitted").getItem(19)).drop("_corrupt_record").drop("splitted")


# select the columns and filter out null
val candidatesDF_resume_space_null = candidatesDF.select("candidateCreationDate", "resume_text").filter(col("resume_text").notEqual("")).filter(col("resume_text").notEqual("null"))

# clean html parse
val df_html = candidatesDF_resume_space_null.withColumn("resume_text_html", regexp_replace(candidatesDF_resume_space_null("resume_text"), "<.+?>", "")).filter(col("resume_text_html").notEqual(""))

# remove punctuation
val df_punctuation = df_html.withColumn("resume_text_cleaned", regexp_replace(df_html("resume_text_html"), """(&#9679|&nbsp|&bull|&#61623|gmail.com|[\p{Punct}&&[^.$]]|8203|159|\b\p{IsLetter}{1,2}\b)\s*""", " ")).filter(col("resume_text_cleaned").notEqual(""))

df_punctuation.show()
"""
+---------------------+--------------------+--------------------+--------------------+
|candidateCreationDate|         resume_text|    resume_text_html| resume_text_cleaned|
+---------------------+--------------------+--------------------+--------------------+
| 2007-01-19 12:36:...|<html><head><meta...|Name:Edward Horan...|Name Edward Horan...|
| 2007-01-19 12:36:...|<html><head><meta...|&nbsp;Gaurava Sha...|  Gaurava Shah1 G...|
| 2007-01-19 12:36:...|Name: Walter Rose...|Name: Walter Rose...|Name Walter Rosen...|
| 2007-01-19 12:36:...|<p>&lt;!--  /* Fo...|&lt;!--  /* Font ...|        Font Defi...|
| 2007-01-19 12:36:...|Name: Irene Hagen...|Name: Irene Hagen...|Name Irene Hagenb...|
| 2007-01-19 12:28:...|<html><head><meta...|EricNorberg18Haml...|EricNorberg18Haml...|
| 2007-01-19 12:28:...|<html><head><meta...|368Ontario Street...|368Ontario Street...|
| 2007-01-19 12:27:...|<html><head><meta...|Name:GROSSMAN DAV...|Name GROSSMAN DAV...|
| 2007-01-19 12:27:...|<html><head><meta...|Name:MANGLANI BHA...|Name MANGLANI BHA...|
| 2007-01-19 12:24:...|<html><head><meta...|3 Riverbank Drive...|3 Riverbank Drive...|
| 2007-01-19 12:20:...|<html><head><meta...|***Shewent on pre...|   Shewent  pregn...|
| 2007-01-19 09:07:...|<html><head><meta...|Position Suitabil...|Position Suitabil...|
| 2007-01-19 09:03:...|<html><head><meta...|EM/LM - SS -4/23&...|      4 23  Posit...|
| 2007-01-18 15:40:...|<html><head><meta...|Position Suitabil...|Position Suitabil...|
| 2007-01-18 15:08:...|<html><head><meta...|3rd submittal on ...|3rd submittal  th...|
| 2007-01-18 14:52:...|<html><head><meta...|PositionSuitabili...|PositionSuitabili...|
| 2007-01-18 14:47:...|<html><head><meta...|201-946-2559(617)...|201 946 2559 617 ...|
| 2007-01-18 14:22:...|<html><head><meta...|646-226-6630vipul...|646 226 6630vipul...|
| 2007-01-18 12:47:...|<p><!-- [if !mso]...|v\\:* {behavior:u...|      behavior ur...|
| 2007-01-18 12:44:...|<html><head><meta...|****Meghe is gett...|    Meghe  gettin...|
+---------------------+--------------------+--------------------+--------------------+
only showing top 20 rows
"""

import org.apache.spark.ml.feature.{RegexTokenizer
                                    import Tokenizer}
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.feature.StopWordsRemover
    import org.apache.spark.ml.feature.NGram
    import org.apache.spark.ml.{Pipeline
                                import PipelineModel}

# import org.apache.spark.sql.SparkSession
# import org.apache.spark.sql.functions._
# import org.apache.spark.ml.feature.StopWordsRemover
# import org.apache.spark.ml.feature.NGram
# import org.apache.spark.ml.{Pipeline, PipelineModel}

# regexTokenizer
val regexTokenizer = new RegexTokenizer().setInputCol("resume_text_cleaned").setOutputCol("resume_text_token").setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

# remove stop words
val remover = new StopWordsRemover().setInputCol("resume_text_token").setOutputCol("resume_text_token_filtered")

# set 2 gram
val ngram = new NGram().setN(2).setInputCol("resume_text_token_filtered").setOutputCol("resume_text_ngrams")

# pipline
val pipeline = new Pipeline().setStages(Array(regexTokenizer, remover, ngram))

# transform from ML lib
val ngramDataFrame = pipeline.fit(df_punctuation).transform(df_punctuation)

"""
+--------------------+--------------------+
|   resume_text_token|  resume_text_ngrams|
+--------------------+--------------------+
|[name, edward, ho...|[name edward, edw...|
|[gaurava, shah1, ...|[gaurava shah1, s...|
|[name, walter, ro...|[name walter, wal...|
|[font, definition...|[font definitions...|
|[name, irene, hag...|[name irene, iren...|
|[ericnorberg18ham...|[ericnorberg18ham...|
|[368ontario, stre...|[368ontario stree...|
|[name, grossman, ...|[name grossman, g...|
|[name, manglani, ...|[name manglani, m...|
|[3, riverbank, dr...|[3 riverbank, riv...|
|[shewent, pregnan...|[shewent pregnanc...|
|[position, suitab...|[position suitabi...|
|[4, 23, positions...|[4 23, 23 positio...|
|[position, suitab...|[position suitabi...|
|[3rd, submittal, ...|[3rd submittal, s...|
|[positionsuitabil...|[positionsuitabil...|
|[201, 946, 2559, ...|[201 946, 946 255...|
|[646, 226, 6630vi...|[646 226, 226 663...|
|[behavior, url, d...|[behavior url, ur...|
|[meghe, getting, ...|[meghe getting, g...|
+--------------------+--------------------+
"""

# concat all words as a corp ,join with "_"
val df_resume_text_cleaned = ngramDataFrame.withColumn("concatenate_list", concat_ws("_", $"resume_text_ngrams"))

# work with date
import java.sql.Date
import org.apache.spark.sql.types.{DateType
                                   import IntegerType}
# import org.apache.spark.sql.types.{DateType, IntegerType}

# make string to date type
val df_resume_text_cleaned1 = df_resume_text_cleaned.withColumn("candidateCreationDate_date", col("candidateCreationDate").cast("date"))


val cleaned_df = df_resume_text_cleaned1.select("candidateCreationDate_date", "concatenate_list")
# save the dataframe
cleaned_df.write.save("project_BDAD/output/cleaned_df")

# load the dataframe
val df_resume_text_cleaned1 = spark.read.load("project_BDAD/output/cleaned_df")

------------------------------------------------------------------------------
#####
# filter by date
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2017-01-01", "2017-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2016-01-01", "2016-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2015-01-01", "2015-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2014-01-01", "2014-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2013-01-01", "2013-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2012-01-01", "2012-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2011-01-01", "2011-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2010-01-01", "2010-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2009-01-01", "2009-12-31"))
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2008-01-01", "2008-12-31"))
------------------------------------------------------------------------------
val df_resume_text_cleaned2 = df_resume_text_cleaned1.filter($"candidateCreationDate_date".between("2007-01-01", "2007-12-31"))
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
------------------------------------------------------------------------------
# create rdd and do the word count
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)

# sort by count number (false:descending)
val wrd_cnt_sort = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)


val wrd_cnt_sort2017 = wrd_cnt.map(item=> item.swap).sortByKey(false, 1).map(item = > item.swap)
wrd_cnt_sort2017.saveAsTextFile("project_BDAD/output/wrd_cnt_sort2017")

sc.parallelize(wrd_cnt_sort).saveAsTextFile("project_BDAD/output/wrd_cnt_sort_test")
"""
res3: Array[(String, Int)] = Array((new york,830), (software engineer,795), (mso level,787), (computer science,733), (sql server,556), (middot developed,520), (designed developed,481), (web services,474), (mso style,430), (real time,411), (front end,411), (font family,406), (software development,404), (middot designed,383), (designed implemented,367), (level number,330), (html css,321), (machine learning,316), (web application,311), (design development,309), (middot worked,299), (asp net,277), (big data,274), (software developer,259), (middot created,245), (management system,241), (senior software,224), (web based,216), (middot implemented,212), (visual studio,212), (test cases,206), (using java,204), (application server,203), (project management,203), (gpa 3,202), (middot used,201), (u...
"""
"""
res4: Array[(String, Int)] = Array((sql server,5561), (new york,4566), (software engineer,4480), (computer science,3960), (middot developed,3723), (web services,2940), (software development,2779), (designed developed,2665), (real time,2450), (middot designed,2391), (designed implemented,2284), (design development,2266), (asp net,2218), (middot worked,2020), (visual studio,2011), (front end,1971), (middot created,1729), (middot implemented,1513), (html css,1480), (web application,1469), (management system,1451), (software developer,1444), (4 0,1409), (web based,1367), (stored procedures,1333), (senior software,1297), (test cases,1292), (object oriented,1256), (using java,1202), (machine learning,1177), (ndash present,1152), (user interface,1152), (middot used,1136), (middot involved,1133...
"""
