import domain.Domain.{Discount, Order, OrderId, Payment, Profile, UserId}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import topics.Topics
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.lang.StackWalker.Option
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object Main {

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    new Serde[A] {
      override def serializer(): Serializer[A] = (topic: String, data: A) => data.asJson.noSpaces.getBytes
      override def deserializer(): Deserializer[A] = (topic: String, data: Array[Byte]) => {
        val string = new String(data)
        val value = decode[A](string)
        value.toOption match {
          case Some(object_) => object_
          case None => throw new RuntimeException("not a valid input")
        }
      }
    }
  }

//  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
//
//    val serializer: Serializer[A] = (topic: String, data: A) => data.asJson.noSpaces.getBytes
//    val deserializer: Deserializer[A] = (topic: String, data: Array[Byte]) => {
//      val string = new String(data)
//      val value = decode[A](string)
//      value.toOption match {
//        case Some(object_) => object_
//        case None => throw new RuntimeException("not a valid input")
//      }
//    }
//
//    Serdes.serdeFrom(serializer, deserializer)
//  }

  def main(args: Array[String]): Unit = {
//    val order = Order(orderId = "2", user = "1", products = List("1"), amount = 100)
//    val json = order.asJson
//    println(s"order: ${json}")

    val builder = new StreamsBuilder()
    val userOrderStream: KStream[UserId, Order] = builder.stream[UserId, Order](Topics.OrdersByUserTopic)
    val profileTable: KTable[UserId, Profile] = builder.table[UserId, Profile](Topics.DiscountProfilesByUserTopic)

    // GlobalKTable
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Topics.DiscountsTopic)

    val expensiveOrders: KStream[UserId, Order] = userOrderStream.filter((userId, order) => order.amount > 1000)
    val listOfProducts: KStream[UserId, List[domain.Domain.Product]] = userOrderStream.mapValues(order => order.products)
    val productsStream: KStream[UserId, domain.Domain.Product] = userOrderStream.flatMapValues(order => order.products)

    val usersWithProfile: KStream[UserId, (Order, Profile)] = userOrderStream.join(profileTable)((order, profile) => (order, profile))
    val discountedStream = usersWithProfile.join(discountProfilesGTable)(
      { case (_, (_, profile: Profile)) => profile },
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) }
    )

    val orderedStream: KStream[OrderId, Order] = discountedStream.selectKey((user, order) => order.orderId)
    val paymentStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](topics.Topics.PaymentsTopic)

    val process = (order: Order, payment: Payment) => if (payment.status == "PAID") scala.Option[Order](order) else scala.Option.empty[Order]
    val paidOrders = orderedStream
      .join(paymentStream)(process, JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES)))
      .filter((_, mayBeOrder) => mayBeOrder.nonEmpty)
      .flatMapValues(mayBeOrder => mayBeOrder.toList)

    paidOrders.to(topics.Topics.PaidOrdersTopic)
    val topology = builder.build()
    val description = topology.describe()
    println(description)

    val props: Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application-final")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    val streamsApp = new KafkaStreams(topology, props)
    streamsApp.start()
  }
}