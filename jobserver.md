#Initiating Job Server
cd /home/maverick/Desktop/repos/spark-jobserver/

sbt
job-server/reStart


#Generating jars
cd /home/maverick/Desktop/repos/spark-jobserver/
sbt package


before uploading job
cd /home/maverick/Desktop/repos/spark-jobserver/job-server-tests/target/scala-2.10


curl --data-binary @job-server-tests_2.10-0.7.0-SNAPSHOT.jar localhost:8090/jars/algorithms
curl --data-binary @job-server-tests_2.10-0.7.0-SNAPSHOT.jar localhost:8090/jars/pagerank
curl --data-binary @job-server-tests_2.10-0.7.0-SNAPSHOT.jar localhost:8090/jars/pr

#Testing pagerank
curl -d "input.string = a b c a b see dimelo" 'localhost:8090/jobs?appName=pagerank&classPath=spark.jobserver.PageRank'
{
  "status": "STARTED",
  "result": {
    "jobId": "b1697a2e-189d-4735-9642-83262958f122",
    "context": "56501fa1-spark.jobserver.PageRank"
  }
}⏎      


classes
cd /home/maverick/Desktop/repos/spark-jobserver/job-server-tests/target/scala-2.10/classes

#result
curl localhost:8090/jobs/5453779a-f004-45fc-a11d-a39dae0f9bf4

ERRORS:
#There was a huge error with hosts and cassandra I had to change rpc_address: 127.0.1.1
#TODO listen in a single address

#First Scripts:
curl --data-binary @job-server-tests/target/scala-2.10/job-server-tests-$VER.jar localhost:8090/jars/test

curl -d "input.string = a b c a b see dimelo" 'localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample'

curl --data-binary @job-server-tests_2.10-0.7.0-SNAPSHOT.jar localhost:8090/jars/test

#spark job server jar
/home/maverick/Desktop/repos/spark-jobserver/job-server-extras/target/scala-2.10/spark-job-server.jar

