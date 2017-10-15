# Kafka Test
## Running
```
docker-compose up -d
run Main.kt
```
## Examples
in Main.kt there is an example using the producer and consumer api  
StreamMain.kt uses the kafka streams api

## bonuses
Kafka web ui: [localhost](http://localhost)  
Zookeeper web ui: [localhost:8080](http://localhost:8080)

## List outdated dependencies
```
./gradlew dependencyUpdates
```
