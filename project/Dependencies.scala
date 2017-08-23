import sbt._

object Dependencies {
  object Versions {
    val kafka = "0.10.0.1"
    val optimus = "2.0.0"
    val scalaz = "7.2.15"
    val circe = "0.8.0"
    val scopt = "3.6.0"
  }

  object Kafka {
    val version = Versions.kafka
    val kafka = "org.apache.kafka" %% "kafka" % version
  }

  object Optimus {
    val version = Versions.optimus
    val core =  "com.github.vagmcs" %% "optimus" % version
    val solverLp = "com.github.vagmcs" %% "optimus-solver-lp" % version
    val oj = "com.github.vagmcs" %% "optimus-solver-oj" % version
  }

  object Scalaz {
    val version = Versions.scalaz
    val core = "org.scalaz" %% "scalaz-core"  % version
  }

  object Circe {
    val version = Versions.circe
    val core = "io.circe" %% "circe-core" % version
    val generic = "io.circe" %% "circe-generic" % version
    val parser = "io.circe" %% "circe-parser" % version
    val all = List(core, generic, parser)
  }

  val Scopt = "com.github.scopt" %% "scopt" % Versions.scopt

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
}
