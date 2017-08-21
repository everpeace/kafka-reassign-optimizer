import Dependencies._
import com.typesafe.sbt.packager.docker._

lazy val root = (project in file(".")).
  enablePlugins(DockerPlugin, JavaAppPackaging).
  settings(
    organization := "com.github.everpeace",
    name := "kafka-reassign-optimizer",
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
    fork in Test := true,
    fork in run  := true,
    connectInput in run := true,
    mainClass in Compile := Some("com.github.everpeace.kafka.reassign_optimizer.Main"),
    dockerBaseImage := "everpeace/lpsolve-java:0.0.1",
    dockerUpdateLatest := true,
    dockerRepository := Some("everpeace"),
    dockerCommands += Cmd("LABEL","maintainer","everpeace <https://github.com/everpeace/>")
  )

import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  runClean,                               // : ReleaseStep
  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  runClean,
  ReleaseStep(releaseStepTask(publish in Docker)),
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)
