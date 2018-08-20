package com.datastax.atwater.loadtest

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.ConsistencyLevel._
import com.datastax.driver.core.{Session => _, _}
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.DseProtocolBuilder
import com.github.javafaker.Faker
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}

import scala.concurrent.duration.FiniteDuration


class AtwaterPerfTest  extends Simulation {
  val clusterBuilder = new DseCluster.Builder

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
  val cluster: DseCluster = clusterBuilder.build()
  val dseSession: DseSession = cluster.connect()

  val cqlConfig: DseProtocolBuilder = dseProtocolBuilder.session(dseSession)
  val insertPrepared: PreparedStatement = dseSession.prepare("insert into ebusiness.product (product_id,color,description,name,price,size,upc) VALUES(?,?,?,?,?,?,?)")



  val faker = new Faker
  val random = new java.util.Random
  val feeder = Iterator.continually({
    Map(
      "product_id" -> UUID.randomUUID(),
      "color" -> faker.color.name,
      "description" -> faker.book.title,
      "name" -> faker.book.author,
      "price" -> faker.number.randomDouble(2,10,100),
      "size" -> faker.code().isbn10,
      "upc" -> faker.code.isbn13
    )})

  val insertProductTable: ChainBuilder =
    exec(
      cql("InsertProduct")
      .executePrepared(insertPrepared)
      .withParams(List("product_id", "color", "description", "name", "price", "size", "upc"))
    )
  val loadData: ScenarioBuilder = scenario("loadData")
    .feed(feeder)
    .exec(insertProductTable)

  val concurrentSessionCount = 20
  val testDuration = 4

  setUp(
    loadData.inject(
      constantUsersPerSec(concurrentSessionCount / 4) during (testDuration / 4),
      nothingFor(FiniteDuration(1, TimeUnit.MINUTES)),
      constantUsersPerSec(concurrentSessionCount) during testDuration
    )
  ).protocols(cqlConfig)

}
