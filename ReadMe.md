. To bulild the uber jar. Do mvn clean package

. To run load test on Product and table insertion Do the following
java -cp target/service.poc.loadtest-1.0-SNAPSHOT.jar -DtestDuration=30 -DconcurrentSessionCount=200 io.gatling.app.Gatling -s com.datastax.atwater.loadtest.PopulateProducts -rf results-product

java -cp target/service.poc.loadtest-1.0-SNAPSHOT.jar -DtestDuration=30 -DconcurrentSessionCount=200 io.gatling.app.Gatling -s com.datastax.atwater.loadtest.PopulateOrders -rf results-orders

The configuration provided will run 30 minutes load with 200 requests/s rate.