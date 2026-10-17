name         := "lakehouse-jobs"
version      := "0.1.0"
scalaVersion := "2.12.18"   // khớp scala-library-2.12.18.jar trong image Spark

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

// Test chạy Spark local trong một JVM riêng. JDK 17 chặn các truy cập nội bộ mà Spark cần,
// nên phải mở các package đó (danh sách theo khuyến nghị của Spark cho JDK 17).
Test / fork := true
Test / javaOptions ++= Seq(
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
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)