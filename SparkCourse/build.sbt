name := "SparkCourse"
organization in ThisBuild := "com.dsv"
scalaVersion in ThisBuild := "2.11.12"

// PROJECTS
lazy val global = project
  .in(file("."))
  .settings(settings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    sparkcore,
    spark16,
    spark24,
    kafka,
    finalproject
  )

lazy val sparkcore = project
  .settings(
    name := "sparkcore",
    settings,
    libraryDependencies ++= Seq(dependencies.sparkcore16)
  )

lazy val kafka = project
  .settings(
    name := "kafka",
    settings,
    libraryDependencies ++= commonDependencies ++ kafkalib
  )

lazy val spark16 = project
  .settings(
    name := "spark16",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ SparkSql16_dependencies
  )

lazy val spark24 = project
  .settings(
    name := "spark24",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Spark24_dependencies
  )

lazy val finalproject = project
  .settings(
    name := "FinalProject",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Project
  )

//DEPENDENCIES
lazy val dependencies =
  new {
    val sparkcore16 = "org.apache.spark" %% "spark-core" % "1.6.3"
    val sparksql16 = "org.apache.spark" %% "spark-sql" % "1.6.3"
    val sparkhive16 = "org.apache.spark" %% "spark-hive" % "1.6.3"
    val sparkcsv16 = "com.databricks" %% "spark-csv" % "1.5.0"
    val sparkxml16 = "com.databricks" %% "spark-xml" % "0.4.1"

    val sparkcore24 = "org.apache.spark" %% "spark-core" % "2.4.4"
    val sparksql24 = "org.apache.spark" %% "spark-sql" % "2.4.4"
    val sparkStreaming24 = "org.apache.spark" %% "spark-streaming" % "2.4.4"
    val sparkml24 = "org.apache.spark" %% "spark-mllib" % "2.4.4"
    val spark_kafka24 = "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
    val spark_hive24 = "org.apache.spark" %% "spark-hive" % "2.4.4"

    val scalatest = "org.scalatest" %% "scalatest" % "3.0.8"
    val mysql = "mysql" % "mysql-connector-java" % "5.1.47"
    val kafka = "org.apache.kafka" %% "kafka" % "2.3.1"
  }

//Common across Project
lazy val commonDependencies = Seq(
  dependencies.scalatest % "test",
  dependencies.mysql
)

//Spark-Core Project Dependencies
lazy val SparkCore_dependencies = Seq(
  dependencies.sparkcore16
)

//Spark 1.6 Project Dependencies
lazy val SparkSql16_dependencies = Seq(
  dependencies.sparkcore16,
  dependencies.sparksql16,
  dependencies.sparkhive16,
  dependencies.sparkcsv16,
  dependencies.sparkxml16
)

//Spark 2.4 Project Dependencies
lazy val Spark24_dependencies = Seq(
  dependencies.sparkcore24,
  dependencies.sparksql24,
  dependencies.sparkml24,
  dependencies.sparkStreaming24,
  dependencies.spark_kafka24,
  dependencies.spark_hive24
)
//Kafka Project Dependencies
lazy val kafkalib = Seq(
  dependencies.kafka
)

//Final Project Dependencies
lazy val Project = Seq(
  dependencies.sparkcore24,
  dependencies.sparksql24,
  dependencies.sparkml24,
  dependencies.sparkStreaming24,
  dependencies.spark_kafka24
)

// SETTINGS
lazy val settings =
  commonSettings ++
    wartremoverSettings ++
    scalafmtSettings

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val wartremoverSettings = Seq(
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.Throw)
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf"            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
