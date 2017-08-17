#!/bin/bash

mvn clean package -Pbenchmarks -DskipTests

java -Djava.library.path=src/main/resources/lib/ -jar target/jnative-1.0-SNAPSHOT.jar -f 2 -i 5 -wi 5 -tu s
