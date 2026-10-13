name         := "lakehouse-jobs"
version      := "0.1.0"
scalaVersion := "2.12.18"   // khớp scala-library-2.12.18.jar trong image Spark

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided