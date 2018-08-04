First, log on the dumbo.

Transfer data from mySQL to dumbo. The flag show my directory on my HDFS: --target-dir ./project_BDAD/candidatesBDAD, you can modify it

candidatesBDAD:
sqoop import --connect jdbc:mysql://avaultdbprod.cjtcotq8rbjh.us-east-1.rds.amazonaws.com/itwallstreet?zeroDateTimeBehavior=CONVERT_TO_NULL --username andiamop --password *Cupertino1 --target-dir ./project_BDAD/candidatesBDAD  --table candidatesBDAD -m 1\

---------------------------------------------------------------------
# test for delimiters
sqoop import --connect jdbc:mysql://avaultdbprod.cjtcotq8rbjh.us-east-1.rds.amazonaws.com/itwallstreet?zeroDateTimeBehavior=CONVERT_TO_NULL --username andiamop --password *Cupertino1 --target-dir ./project_BDAD/candidatesBDAD  --table candidatesBDAD --split-by candidateId --mysql-delimiters

-----------------------------------------------------------------------

jobOrdersBDAD:
sqoop import --connect jdbc:mysql://avaultdbprod.cjtcotq8rbjh.us-east-1.rds.amazonaws.com/itwallstreet?zeroDateTimeBehavior=CONVERT_TO_NULL --username andiamop --password *Cupertino1 --target-dir ./project_BDAD/jobOrdersBDAD  --table jobOrdersBDAD -m 1\


placementsBDAD:
sqoop import --connect jdbc:mysql://avaultdbprod.cjtcotq8rbjh.us-east-1.rds.amazonaws.com/itwallstreet?zeroDateTimeBehavior=CONVERT_TO_NULL --username andiamop --password *Cupertino1 --target-dir ./project_BDAD/placementsBDAD  --table  placementsBDAD -m 1\


submittalTrackingBDAD:
sqoop import --connect jdbc:mysql://avaultdbprod.cjtcotq8rbjh.us-east-1.rds.amazonaws.com/itwallstreet?zeroDateTimeBehavior=CONVERT_TO_NULL --username andiamop --password *Cupertino1 --target-dir ./project_BDAD/submittalTrackingBDAD  --table submittalTrackingBDAD -m 1\

