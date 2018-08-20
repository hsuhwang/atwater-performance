package com.datastax.atwater.loadtest

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel._
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{Session => _, _}
import com.datastax.driver.dse.DseCluster
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.DseProtocolBuilder
import com.github.javafaker.Faker
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}

class AtwaterProductToOrdersLoad extends Simulation {

  val clusterBuilder = new DseCluster.Builder
  setUpClusterBuilder()

  val cluster: DseCluster = clusterBuilder.build()
  val dseSession = cluster.connect()
  val cqlConfig: DseProtocolBuilder = dseProtocolBuilder.session(dseSession)
  //val insertPrepared: PreparedStatement = dseSession.prepare("insert into ebusiness.orders (order_id,customer_id,product_id,order_status,customer_address,customer_name,color,product_description,product_name,price,quantity,size,upc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
  val selectUPC: SimpleStatement = new SimpleStatement(("SELECT upc from ebusiness.orders"))
  val feeder = setupFeeder()
  startSimulation(feeder)


  def setUpClusterBuilder(): Unit = {
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


  def startSimulation(feeder: Iterator[Map[String, Any]]): Unit = {
    val getUPC: ChainBuilder =
      exec(
        cql("selectUPC")
          .executeStatement(selectUPC).check(resultSet.saveAs("fetchedData"))
      ).exec({session: Session =>
        val fetchedData = session("fetchedData").as[ResultSet].one
        // do something with fetched data...
        session
      })
    val loadData: ScenarioBuilder = scenario("loadData")
      .feed(feeder)
      .exec(getUPC)
    setUp(loadData.inject(atOnceUsers(1) )
    ).protocols(cqlConfig)
  }

  def setupFeeder(): Iterator[Map[String, Any]] = {
    val faker = new Faker
    val feeder = Iterator.continually({
      Map(
        "order_id" -> UUIDs.timeBased(),
        "customer_id" -> UUID.randomUUID(),
        "product_id" -> UUID.randomUUID(),
        "order_status" -> faker.color.name,
        "customer_address" -> faker.address.fullAddress(),
        "customer_name" -> faker.name().fullName(),
        "color" -> faker.color.name,
        "product_description" -> faker.book.title,
        "product_name" -> faker.book.author,
        "price" -> faker.number.randomDouble(2, 10, 100),
        "quantity" -> faker.number.numberBetween(1, 9),
        "size" -> faker.code().isbn10,
        "upc" -> faker.code.isbn13
      )
    })
    feeder
  }

}
