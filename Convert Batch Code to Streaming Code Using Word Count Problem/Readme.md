# Read real-time Streaming data from Socket and write to Console Using Word Count Problem in Spark

## Sokect endpoint (ncat) --> input passed as small batch-by-batch --> Input goes through SPARK session word count code logic. --> Output is displayed on Docker jupyter lab container logs.

## STEPS:
- Read streaming data from Sockets.
- Convert Batch code to Streaming.
- Use word count problem.

We will use NCAT command line utility tool for testing socket streaming application.
NCAT will create the endpoint, which will be use by SPARK for streaming data.

- Once the Docker is up and running. Connect to the Jupyter Lab container using CLI.
- Write below command: (docker exec -it container_id /bin/bash). It will open the bash shell for us.
- Update the container first by (sudo apt-get update)
- Install NCAT by (sudo apt-get install ncat). We will use this ncat to open socket endpoint. Keep this CLI terminal open to send streaming inputs.
- Now connect to the Jupyter Lab environment by (localhost://8888)
- Create .ipynb file and write the code for reading data from sockets.
- First we write this program for streaming.
  - You can use example.txt as data file for BATCH processing. But we will count the words from stream processing in real-time for this project.
  - Keep Spark UI open in browser by side using (localhost://4040)
  - So once we run the code, it will split the line into words and put it in a list.
  - Use explode() method to put each word into a single row.
  - Now aggregate the word count.
  - Write the streaming data to the console terminal to display.

- Open the logs on the docker jupyter container.
- Go to the terminal for that we opened for NCAT earliar.
- Here open the socket endpoint by (ncat -l 9999)
- Run the final code line first from jupyter lab.
- Now if you go to the docker container jupyter logs. It will show first batch-0 processing there.
- Now go to the Terminal shell and provide some inputs like (hello world).
