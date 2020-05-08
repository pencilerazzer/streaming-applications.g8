val user_uri = sys.env.get("USER") match {
  case Some(value) => s"/home/\${value}"
  case _ => "/root"
}

resolvers += "Akka Snapshots" at "https://repo.akka.io/snapshots/"
resolvers += Resolver.file("cloudflow", file(s"\${user_uri}/.ivy2/local/com.lightbend.cloudflow/sbt-cloudflow"))(Resolver.ivyStylePatterns)
addSbtPlugin("com.lightbend.cloudflow" % "sbt-cloudflow" % "1.3.4-SNAPSHOT")