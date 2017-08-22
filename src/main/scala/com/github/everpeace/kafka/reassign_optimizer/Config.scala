package com.github.everpeace.kafka.reassign_optimizer

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

case class Config(
                   zkString: String ="",
                   newBrokers: Set[Int] = Set.empty,
                   topics: Set[String] = Set.empty,
                   printAssignment: Boolean = false,
                   balancedFactorMin: Double = 0.9d,
                   balancedFactorMax: Double = 1.0d,
                   execute: Boolean = false,
                   batchWeight: Int = 0, // 0 means execute all re-assignment at once.
                   verify: Boolean = true,
                   verifyInterval: Duration = 5 seconds,
                   verifyTimeout: Duration = 5 minutes
                 )

object Config {
  val parser = new scopt.OptionParser[Config]("kafka-reassign-optimizer") {
    val default = Config()

    head("kafka-reassign-optimizer", "calculating balanced partition replica reassignment but minimum replica move.")
    opt[String]("zookeeper").abbr("zk").required()
      .text("zookeeper connect string (e.g. zk1:2181,zk2:2181/root )")
      .action { (zk, c) => c.copy(zkString = zk) }

    opt[Seq[Int]]("brokers").required()
      .text("broker ids to which replicas re distributed to")
      .valueName("id1,id2,...")
      .action((brokers, c) => c.copy(newBrokers = brokers.toSet))

    opt[Seq[String]]("topics")
      .text("target topics (all topics when not specified)")
      .valueName("topic1,topic2,...")
      .action((topics, c) => c.copy(topics = topics.toSet))

    opt[Unit]("print-assignment")
      .text(s"print assignment matrix. please noted this might make huge output when there are a lot topic-partitions (default = ${default.printAssignment})")
      .action((_, c) => c.copy(printAssignment = true))

    opt[Double]("balanced-factor-min")
      .text(s"stretch factor to decide new assignment is well-balanced (must be <= 1.0, default = ${default.balancedFactorMin})")
      .validate(f =>
        if (f <= 1.0) success
        else failure("balanced-factor-min must be <= 1.0")
      ).action((f, c) => c.copy(balancedFactorMin = f))

    opt[Double]("balanced-factor-max")
      .text(s"stretch factor to decide new assignment is well-balanced (must be >= 1.0, default = ${default.balancedFactorMax})")
      .validate(f =>
        if (f >= 1.0) success
        else failure("balanced-factor-max must be >= 1.0")
      ).action((f, c) => c.copy(balancedFactorMax = f))

    opt[Unit]('e', "execute")
      .text(s"execute re-assignment when found solution is optimal (default = ${Config().execute})")
      .action((_, c) => c.copy(execute = true))

    opt[Int]("batch-weight")
      .text(s"execute re-assignment in batch mode (default = ${Config().batchWeight} (0 means execute re-assignment all at once))")
      .validate( b =>
        if( b >= 0 ) success
        else failure("batch-weight must not be negative.")
      ).action((b,c) => c.copy(batchWeight =  b))

    opt[Boolean]("verify")
      .text(s"verifying reassignment finished after execution of reassignment fired (default = ${Config().verify}). this option is active only when execution is on.")
      .action((v, c) => c.copy(verify = v))

    opt[Duration]("verify-interval")
      .text(s"interval duration of verifying reassignment execution progress (default = ${Config().verifyInterval})")
      .valueName("(e.g. 1s, 1min,..)")
      .action((d, c) => c.copy(verifyInterval = d))

    opt[Duration]("verify-timeout")
      .text(s"timeout duration of verifying reassignment execution progress (default = ${Config().verifyTimeout})")
      .valueName("(e.g. 1m, 1hour,..)")
      .action((d, c) => c.copy(verifyTimeout = d))

    help("help").text("prints this usage text")
  }
}