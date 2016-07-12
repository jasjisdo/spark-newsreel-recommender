#!/bin/sh

cd /mnt/algo
EVAL_FILE=clef-idomaar-2013-10-10.log

# START SERVER
echo "try to start the recommender server"
java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar de.dailab.plistacontest.client.ClientAndContestHandler0MQ 0.0.0.0:8088 log4j.properties &
sleep 5

# START THE SENDER
echo "try to start the evaluation run"
java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.sender.RequestSenderZMQ "http://127.0.0.1:8088" $EVAL_FILE

# TEST RUN COMPLETED
echo "recommendation run using file $EVAL_FILE finished"