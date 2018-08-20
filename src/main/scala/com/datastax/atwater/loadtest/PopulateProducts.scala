package com.datastax.atwater.loadtest

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.ConsistencyLevel._
import com.datastax.driver.core.{Session => _, _}
import com.datastax.driver.dse.DseCluster
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.DseProtocolBuilder
import com.github.javafaker.Faker
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}

import scala.concurrent.duration.FiniteDuration

class PopulateProducts  extends Simulation {

  val clusterBuilder = new DseCluster.Builder
  setUpClusterBuilder()

  val cluster: DseCluster = clusterBuilder.build()
  val dseSession = cluster.connect()
  val cqlConfig: DseProtocolBuilder = dseProtocolBuilder.session(dseSession)
  val insertPrepared: PreparedStatement = dseSession.prepare("insert into ebusiness.product (product_id,color,description,name,price,size,upc) VALUES(?,?,?,?,?,?,?)")
  val feeder = setupFeeder()
  startSimulation(feeder)

  def setUpClusterBuilder():Unit = {
    clusterBuilder.
      withSocketOptions(new SocketOptions().
        setKeepAlive(true).
        setTcpNoDelay(true)).
      withQueryOptions(new QueryOptions().
        setDefaultIdempotence(true).
        setPrepareOnAllHosts(true).
        setReprepareOnUp(true).
        setConsistencyLevel(LOCAL_ONE)).
      withPoolingOptions(new PoolingOptions().
        setCoreConnectionsPerHost(HostDistance.LOCAL, 1).
        setMaxConnectionsPerHost(HostDistance.LOCAL, 2).
        setNewConnectionThreshold(HostDistance.LOCAL, 30000).
        setMaxRequestsPerConnection(HostDistance.LOCAL, 30000))
    System.getProperty("contactPoints", "34.229.153.242").split(",")
      .foreach(clusterBuilder.addContactPoint)
  }


def startSimulation(feeder:Iterator[Map[String,Any]]):Unit ={
    val insertProductTable: ChainBuilder =
      exec(
        cql("InsertProduct")
          .executePrepared(insertPrepared)
          .withParams(List("product_id", "color", "description", "name", "price", "size", "upc"))
      )
    val loadData: ScenarioBuilder = scenario("loadData")
      .feed(feeder)
      .exec(insertProductTable)

    val concurrentSessionCount = System.getProperty("concurrentSessionCount").toInt
    val testDuration = System.getProperty("testDuration").toInt

    setUp(loadData.inject(constantUsersPerSec(concurrentSessionCount) during (FiniteDuration(30,TimeUnit.MINUTES)))).throttle(
      reachRps(concurrentSessionCount) in (FiniteDuration(10,TimeUnit.SECONDS)),
      holdFor(FiniteDuration(testDuration , TimeUnit.MINUTES))
    ).protocols(cqlConfig)
  }

  def setupFeeder(): Iterator[Map[String,Any]]={
    val faker = new Faker
    val feeder = Iterator.continually({
      Map(
        "product_id" -> UUID.randomUUID(),
        "color" -> faker.color.name,
        "description" -> faker.book.title,
        "name" -> faker.book.author,
        "price" -> faker.number.randomDouble(2, 10, 100),
        "size" -> faker.code().isbn10,
        "upc" -> faker.code.isbn13
      )
    })
    feeder
  }

}
