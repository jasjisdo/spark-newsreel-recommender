#!/bin/sh

cd /mnt/algo

# START SERVER
echo "try to start the recommender server"
java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar de.dailab.plistacontest.client.Client 0.0.0.0:8088 log4j.properties &
sleep 5

# START THE SENDER
echo "try to start the evaluation run"
java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.sender.RequestSender "http://127.0.0.1:8088" clef-idomaar-2013-10-10.log

# TEST RUN COMPLETED
echo "test run finished"

IPA=`netstat -rn | grep "^0.0.0.0 " | cut -d " " -f10`
java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.control.StringSender "tcp://$IPA:2760" READY
echo "sent READY finished"

java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.control.StringSender "tcp://$IPA:2760" OK
echo "sent OK0 finished"

java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.control.StringSender "tcp://$IPA:2760" OK
echo "sent OK1 finished"

java -cp target/clef-newsreel-challenge-0.0.1-jar-with-dependencies.jar eu.crowdrec.contest.control.StringSender "tcp://$IPA:2760" OK
echo "sent OK2 finished"
