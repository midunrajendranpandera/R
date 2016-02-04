library('rJava')
.jinit()
.jaddClassPath("r-mongo-scala-1.0-SNAPSHOT.jar")

setClass("RMongo", representation(javaMongo = "jobjRef"))

mongoDbConnect <- function(dbName, host="127.0.0.1", port=27017){
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, host, as.integer(port)))
  rmongo
}

mongoDbReplicaSetConnect <- function(dbName, hosts="127.0.0.1:27017"){
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, hosts, FALSE))
  dbDisconnect(rmongo)
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, hosts, TRUE))
  rmongo
}

mongoDbReplicaSetConnectWithCredentials <- function(dbName, hosts="127.0.0.1:27017", username, pwd){
rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, hosts, TRUE, username, pwd))
  rmongo
}

setGeneric("dbAuthenticate", function(rmongo.object, username, password) standardGeneric("dbAuthenticate"))
setMethod("dbAuthenticate", signature(rmongo.object="RMongo", username="character", password="character"),
   function(rmongo.object, username, password){
    results <- .jcall(rmongo.object@javaMongo, "Z", "dbAuthenticate", username, password)
    results
  }
)
