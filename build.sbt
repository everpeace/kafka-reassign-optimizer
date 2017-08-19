import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(DockerPlugin, JavaAppPackaging).
  settings(
    organization := "com.github.everpeace",
    name := "kafka-reassign-optimizer",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      Scopt,
      Scalaz.core,
      Kafka.kafka,
      Optimus.core,
      Optimus.solverLp
    ) ++ Circe.all ++ Seq(
      scalaTest % Test
    ),
    mainClass in Compile := Some("com.github.everpeace.kafka.reassign_optimizer.Main"),
    dockerBaseImage := "everpeace/lpsolve-java:0.0.1",
    dockerUpdateLatest := true,
    dockerRepository := Some("everpeace")
  )