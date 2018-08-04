# data-driven-recruting
This project focus on HR data. We explore the resume data of each job candidates. Investigating the “buzz” words that were being used in resumes on any given time scale. It gives user insight how a hot resume should look like. Users also can see the trend of buzz words. By leveraging the scalability of big data platform, we believe that analysis over labor market data will provides excellent insights for recruiters.

***

## about the files:
`BDADFrontEnd.html`: the designed of front end
###### A note about the front end/back end connection.
We were able to extract output in the form of graphs that we would've liked to show as the output on our web app. We have what our ideal look was, BDADFrontEnd.html, but thinking it through, to have users click buttons, then make those lengthy calls to scala, isn't a grea user experience. So after expirementing with a python webapp (Flask), and the Play Framework for scala, we ended up not having a great representation of our front end. Ending with what we'd like our front end to look like, and concluding that all graphs should be preloaded, and merely displayed, rather than recalculated.

`BDAD_output`: the folder include all examples of outputs.


`newplot.png`: an example of bar chart of word trend.  
`main.scala`: main function of this project, we run this script in the Perl.


