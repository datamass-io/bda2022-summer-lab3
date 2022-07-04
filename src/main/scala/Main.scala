import com.typesafe.scalalogging.LazyLogging
import com.azure.cosmos.{ConsistencyLevel, CosmosClient, CosmosClientBuilder, CosmosContainer, CosmosDatabase, CosmosException}
import com.azure.cosmos.models.PartitionKey

import scala.jdk.CollectionConverters.SeqHasAsJava

object Main extends LazyLogging {

  private val accountHost = System.getenv("ACCOUNT_HOST")
  private val masterKey = System.getenv("ACCOUNT_KEY")

  private val databaseName = "Shop"
  private val containerName = "Stock"

  def initDB(): (CosmosClient, CosmosContainer) = {
    val client = new CosmosClientBuilder()
      .endpoint(accountHost).key(masterKey)
      .preferredRegions(List("South UK").asJava)
      .consistencyLevel(ConsistencyLevel.EVENTUAL).buildClient

    //  </CreateSyncClient>
    val database = createDatabaseIfNotExists(client)
    val container = createContainerIfNotExists(database)
    (client, container)
  }


  @throws[Exception]
  private def createDatabaseIfNotExists(client: CosmosClient): CosmosDatabase = {
    logger.info("Create database {} if not exists.", databaseName)

    /** * Create if not exists and return database
     *
     * YOUR CODE HERE
     *
     */
    // return database
  }

  @throws[Exception]
  private def createContainerIfNotExists(database: CosmosDatabase): CosmosContainer = {
    logger.info("Create container {} if not exists.", containerName)
    //
    /*** Create if not exists and return container
     *
     * YOUR CODE HERE
     *
     */
    // return container
  }

  private def createClothing(container: CosmosContainer): List[Clothing] = {
    val clothingToCreate = List[Clothing](
      new Clothing("1", "shirt", "typical shirt"),
      new Clothing("2", "trousers", "strange trousers"),
      new Clothing("3", "shoes", "funny shoes")
    )
    /*** Create items from the list in the database container, and compute the totalRequestCharge
     *
     * YOUR CODE HERE
     *
     */
    logger.info("Created {} items with total request charge of {}",
      clothingToCreate.size,
      totalRequestCharge)
    clothingToCreate
  }

  private def readItems(clothingList: List[Clothing], container: CosmosContainer): Unit = {
    clothingList.foreach(clothing => {
      try {
        val item = container.readItem(clothing.getId, new PartitionKey(clothing.getName), classOf[Clothing])
        val requestCharge = item.getRequestCharge
        val requestLatency = item.getDuration
        logger.info("Item successfully read with id {} with a charge of {} and within duration {}", item.getItem.getId, requestCharge, requestLatency)
      } catch {
        case e: CosmosException =>
          logger.error("Read Item failed with", e)
      }
    })
  }


  private def queryItems(container: CosmosContainer): Unit = { //  <QueryItems>
    /*** Query and output to the logger.info items with name 'shirt' or 'shoes'. Print request charge.
     *
     * YOUR CODE HERE
     *
     */
  }

  def main(args: Array[String]): Unit = {
    var clientOption = None: Option[CosmosClient]
    try {
      logger.info("Hello Azure Cosmos DB")
      val (client, container) = initDB()
      clientOption = Some(client)
      val clothingList = createClothing(container)
      readItems(clothingList, container)
      queryItems(container)
      logger.info("Goodbye")
    } catch {
      case e: Exception =>
        logger.error("CosmosDB failed with", e)
    } finally {
      logger.info("Closing the client")
      clientOption match {
        case Some(client) => client.close()
        case None => logger.warn("Client not created!")
      }
    }
    System.exit(0)
  }
}
