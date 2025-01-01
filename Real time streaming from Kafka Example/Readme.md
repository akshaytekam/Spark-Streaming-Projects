# Real time streaming from Kafka to the jupyter console using spark

### Read the json payload from Kafka topic --> Explode & Flatten this data into dataframe --> using spark show the topic data to the console in real-time.  

## Content:
- Read streaming data from KAFKA
- Flatten and capture device data

## STEPS:
- Go to jupyter lab and start spark session with .config for installing jar packages. (in order to support kafka, we need this)
- First connect to the Kafka container using terminal/cli.
- And create a new topic name (device-data)
    - connect to the kafka container: (docker exec -it container_id /bin/bash)
    - check topic list: (kafka-topics --list --bootstrap-server localhost:29092)
    - Create new topic: (kafka-topics --create --topic device-data --bootstrap-server localhost:29092)
- Create the producer to send the data: (kafka-console-producer --topic device-data --bootstrap-server localhost:29092)
- Now put some sample input data in the terminal, some json payload data. It will be stored in new topic 'device-data'
- And run the kafka_df code from jupyter lab.
- Now once we have the key and value columns data, convert the value binary column into string data type.
- Now create the schema and apply the schema to payload to read the data.
- Now that we have our data with us select the specific columns and explode and flatten the data.
- And at last write the output to the console.
