#Akka jdbc

Akka jdbc is a dsl to do jdbc call with akka stream. 

## Getting starting

```java 

Database database = Database.from(datasource);

ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

Sql sql = database.sql();

//Getting a akka stream source
Source<Superhero, ?> source = sql
        .select("select * from superhero where city_id = ?")
        .param(3)
        .as(Superhero::convert)
        .get();

//Execute the query and collect the results
CompletableFuture<List<Superhero>> superheroes = source.runWith(Sink.seq(), materializer).toCompletableFuture();

superheroes.whenComplete((heroes, error) -> {
    System.out.println(heroes); 
});

```

## Handling Connections

The Sql object got a unique jdbc connection. The default is to close the connection at the end on the query. 

Chaining queries on the same connection could be done like this : 
  
```java 
//The connection stay opened : 
Sql sql = database.sql().keepConnectionOpened();

//Getting the source : 
Source<City, ?> cities = sql
    .select("select * from superhero")
    .as(Superhero::convert)
    .get()
    //Chain with another query : 
    .via(sql.select("select * from city where city_id = ? ")
            .as(City::convert)
            //Use the first query result as parameter :   
            .<Superhero>grabInParam(s -> s.city_id)
    )
    //Close the connection at the end : 
    .via(sql.atTheEnd(Sql::closeConnection));

```

To use a new connection on each query : 

```java 


Source<Superhero, ?> source = database.sql()
        .select("select * from superhero where city_id = ?")
        .param(3)
        .get(Superhero::convert);
        
//Select on new connection : 
Source<Superhero, ?> source2 = database.sql()
        .select("select * from superhero where city_id = ?")
        .param(3)
        .get(Superhero::convert);


```

