# Automatically reading streaming data from Kafka

## Content:
- Automate streaming data from kafka
- Triggers in spark streaming (Once, Processingtime, Continuous)
- Tune kafka streaming job

## STEPS:
- First install the kafka-python library for this task in jupyter terminal(pip install kafka-python)
- Now run the spark session first and flatten the data.
- While writting the output to console, we can trigger the automated process (Once or Processingtime or Continuous)
- Here first we have to run the post_to_kafka.py file in jupyter shell/terminal by (python post_to_kafka.py)
- This above python file will aoutomatically generate data.
- Now run the output streaming query to see the output data on console.
- Further we can tune the job by increasing the number of partitions so that we can achive parallelism.(means incease the topics)
