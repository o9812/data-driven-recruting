# data-driven-recruting
This project focus on HR data. We explore the resume data of each job candidates. Investigating the “buzz” words that were being used in resumes on any given time scale. It gives user insight how a hot resume should look like. Users also can see the trend of buzz words. By leveraging the scalability of big data platform, we believe that analysis over labor market data will provides excellent insights for recruiters.

***

## about the files:
`BDADFrontEnd.html`: the designed of front end
###### A note about the front end/back end connection.
We were able to extract output in the form of graphs that we would've liked to show as the output on our web app. We have what our ideal look was, BDADFrontEnd.html, but thinking it through, to have users click buttons, then make those lengthy calls to scala, isn't a grea user experience. So after expirementing with a python webapp (Flask), and the Play Framework for scala, we ended up not having a great representation of our front end. Ending with what we'd like our front end to look like, and concluding that all graphs should be preloaded, and merely displayed, rather than recalculated.

`BDAD_output`: the folder include all examples of outputs.
- `bar_chart`: bar charts of word distribution from 2007 to 2017/over 10 years.
- `word_cloud`: wordcloud the font size represent the word frequency each year from 2007 to 2017/over 10 years.
![an example of bar chart of word trend](https://github.com/o9812/data-driven-recruting/blob/master/BDAD_output/word_cloud/wrd_cnt_sort_2017.png)
- `word_cnt`: the word count pairs outputs of spark from 2007 to 2017/over 10 years. ex: (software engineer,795), (mso level,787), (computer science,733), (sql server,556), (middot developed,520), (designed developed,481), (web services,474), (mso style,430), (front end,411), (real time,411), (font family,406), (software development,404)..
- `newplot.png`: an example of bar chart of word trend.  
![an example of bar chart of word trend](https://github.com/o9812/data-driven-recruting/blob/master/BDAD_output/newplot.png)


`main.scala`: main function of this project, we run this script in the Perl.

`<asdfdsaf>`
