# Distributed Agreement - Project for Distributed Systems at UT Austin Fall 2019
## Javier Palomares and Matt Molter

### Running Server
First compile

```mvn clean compile```

Then execute

``` mvn exec:java -Dexec.mainClass="distributed.DistributedAgreement" -Dlog4j.configuration=file:"./src/main/resources/log4j.properties" -Dexec.args="./src/main/resources/hosts.yaml <serverId>"```


### Running Client
First compile

```mvn clean compile```

Then execute


``` mvn exec:java -Dexec.mainClass="distributed.client.Client" -Dlog4j.configuration=file:"./src/main/resources/log4j.properties" -Dexec.args="./src/main/resources/hosts.yaml"```
