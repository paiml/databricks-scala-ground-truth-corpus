val sparkVersion = "3.5.4"
val deltaVersion = "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "databricks-scala-ground-truth-corpus",
    version := "0.1.0",
    scalaVersion := "2.12.20",
    organization := "com.paiml",
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),

    libraryDependencies ++= Seq(
      // Spark core
      "org.apache.spark" %% "spark-core"      % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"        % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib"      % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming"  % sparkVersion % "provided",

      // Delta Lake
      "io.delta" %% "delta-spark" % deltaVersion % "provided",

      // Testing
      "org.scalatest"     %% "scalatest"         % "3.2.19"   % Test,
      "org.scalatestplus" %% "scalacheck-1-18"   % "3.2.19.0" % Test,
      "org.scalacheck"    %% "scalacheck"        % "1.18.1"   % Test,

      // Spark for tests (not "provided" scope)
      "org.apache.spark" %% "spark-core"      % sparkVersion % Test,
      "org.apache.spark" %% "spark-sql"        % sparkVersion % Test,
      "org.apache.spark" %% "spark-mllib"      % sparkVersion % Test,
      "org.apache.spark" %% "spark-streaming"  % sparkVersion % Test,
      "io.delta"         %% "delta-spark"      % deltaVersion % Test,
    ),

    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
    ),

    // Test options
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "-XX:+UseG1GC",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ),

    // Coverage settings
    coverageMinimumStmtTotal := 95,
    coverageFailOnMinimum := true,
    coverageHighlighting := true,
  )
