# data-driven-recruting
This project focus on HR data. We explore the resume data of each job candidates. Investigating the “buzz” words that were being used in resumes on any given time scale. It gives user insight how a hot resume should look like. Users also can see the trend of buzz words. By leveraging the scalability of big data platform, we believe that analysis over labor market data will provides excellent insights for recruiters.

***

## about the files:
`BDADFrontEnd.html`: the designed of front end
###### A note about the front end/back end connection.
We were able to extract output in the form of graphs that we would've liked to show as the output on our web app. We have what our ideal look was, BDADFrontEnd.html, but thinking it through, to have users click buttons, then make those lengthy calls to scala, isn't a grea user experience. So after expirementing with a python webapp (Flask), and the Play Framework for scala, we ended up not having a great representation of our front end. Ending with what we'd like our front end to look like, and concluding that all graphs should be preloaded, and merely displayed, rather than recalculated.

`BDAD_output`: the folder include all examples of outputs.
- `bar_chart`: bar charts of word distribution from 2007 to 2017/over 10 years.
- `word_cloud`: wordcloud. The font size represents count numbers from 2007 to 2017/over 10 years. Here is an example in 2017.
![an example of bar chart of word trend](https://github.com/o9812/data-driven-recruting/blob/master/BDAD_output/word_cloud/wrd_cnt_sort_2017.png)
- `word_cnt`: the word count pairs outputs of spark from 2007 to 2017/over 10 years. ex: (software engineer,795), (mso level,787), (computer science,733), (sql server,556), (middot developed,520), (designed developed,481), (web services,474), (mso style,430), (front end,411), (real time,411), (font family,406), (software development,404)..
- `newplot.png`: an example of bar chart of word trend.  
![an example of bar chart of word trend](https://github.com/o9812/data-driven-recruting/blob/master/BDAD_output/newplot.png)

`sqoop_data.sh`: sqoop command line. transder data from external MySQL server to Dumbo HDFS.
`bada_visualization.ipynb`: the notebook is to do the visualizatin and plots. Mainly using ```plotly``` external tool.
***
`main.scala`: main function of this project, run this script in the Perl.

examples:

> load data into as sql.datafram
```
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
```

> do the column operation: regextokenizer, stopwords, bigram
```
val ngramDataFrame = pipeline.fit(df_punctuation).transform(df_punctuation)
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
```

> word count pairs 2017:
```
val wrd_cnt = df_resume_text_cleaned2.select("concatenate_list").rdd.map(x= > x.mkString.split("_")).flatMap(x= > (x)).filter(x = > x != "new york").map(x= > (x, 1)).reduceByKey((v1, v2) = > v1 + v2)

(software engineer,795)
(mso level,787)
(computer science,733)
(sql server,556)
(middot developed,520)
(designed developed,481)
(web services,474)
(mso style,430)
(front end,411)
(real time,411)
(font family,406)
(software development,404)
(active directory,153)
(best practices,152)
(data warehouse,152)
(technical skills,150)
(third party,149)
(operating systems,149)
(design patterns,148)
(middot managed,147)
(unit testing,146)
(web service,144)
(css javascript,143)
(application development,143)
(new roman,173)
(end end,169)
(years experience,168)
(web applications,168)
(back end,167)
(java j2ee,166)
(number position,165)
(level tab,165)
(level text,165)
(format bullet,165)
(bullet mso,165)
(ndash may,165)
(tab stop,165)
(number format,165)
(position left,165)
(javascript jquery,164)
(team members,163)
(project manager,161)
(design implementation,159)
(text mso,158)
(institute technology,157)
(middot experience,156)
(stop none,156)
(open source,154)
(left margin,154)
(middot involved,192)
(ndash present,191)
(professional experience,190)
(margin left,186)
(stored procedures,186)
(object oriented,184)
(science computer,184)
(middot responsible,181)
(none mso,175)
...
```
