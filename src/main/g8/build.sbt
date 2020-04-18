import sbt._
import sbt.Keys._

lazy val `$name;format="norm"$` =  (project in file("."))
  .enablePlugins(CloudflowAkkaStreamsApplicationPlugin, CloudflowSparkApplicationPlugin)
  .settings(
    sourceGenerators in Compile += Def.taskDyn {
        val outFile = sourceManaged.in(Compile).value / "$name$" / "Generated.scala"
        println(outFile)
        Def.task {
          (run in codegen in Compile)
            .toTask(" " + outFile.getAbsolutePath)
            .value
          Seq(outFile)
        }
      }.taskValue,
    name := "$name$"
    )
    .settings(commonSettings)

lazy val commonSettings = Seq(
  organization := "pencilerazer",
  headerLicense := Some(HeaderLicense.ALv2("(C) 2016-2020", "Lightbend Inc. <https://www.lightbend.com>")),
  scalaVersion := "2.12.10",
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8",
    "-Xlog-reflective-calls",
    "-Xlint",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-deprecation",
    "-feature",
    "-language:_",
    "-unchecked"
  ),

  scalacOptions in (Compile, console) --= Seq("-Ywarn-unused", "-Ywarn-unused-import"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)