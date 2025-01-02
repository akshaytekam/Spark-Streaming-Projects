# Automatically reading streaming data from Kafka

### Automatically generating streaming data using python code --> Put that data on kafka topic using python script only --> Now using spark process the data, flatten it and display/sink on the jupyter console. --> Further we can tune it to incease performance

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

![Screenshot 2025-01-02 120529](https://github.com/user-attachments/assets/09ec573f-ae2a-4ecc-bf27-5447d06407f1)
![Screenshot 2025-01-02 120359](https://github.com/user-attachments/assets/b883c710-f762-4f5d-9699-8e3f3ecdfae9)
![Screenshot 2025-01-02 120434](https://github.com/user-attachments/assets/ce9f457b-9f6f-428d-a137-5a9ba7f1acaf)
