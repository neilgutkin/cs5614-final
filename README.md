# CS5614 Final Project: Team 2

# 1. Introduction
This repository holds all the code needed to perform our final project. The sections that will be
outlined are
1. Dataset
2. Environment Setup
3. Unsupervised K-Means
4. (Supervised) SVM

# 2. Dataset
Please Download the dataset ```currentDatasetFinal.zip``` from the link [here](https://github.com/neilgutkin/cs5614-final/tree/main/dataset).
Extract the dataset and place it in the ```/dataset``` directory.


# 3. Environment Setup
This project assumes that both Java and Scala are installed. Additionally, docker containers are used
to create a local Spark session. To start up Docker, please execute the following command

```docker compose -f ./docker-compose-expose.yml up --detach```

Now, the producer needs to be started up. to do this, execute
```sbt run``` and select "Producer".
If all goes well you should see the output that looks similar to the following
```
{"feel":"27.42","mslp":"-1.0","sped":"4.6","drct":"240.0","dwpf":"25.7","ice_acceretion_1hr":"M","relh":"77.28","skyc1":"OVC","p01m":"0.00","valid":"2013-01-04 19:55","alti":"29.71","skyl1":"3200.0","vsby":"10.0","wxcodes":"M","tmpf":"32.0","station":"FSO"}
{"feel":"27.42","mslp":"-1.0","sped":"4.6","drct":"240.0","dwpf":"24.08","ice_acceretion_1hr":"M","relh":"72.24","skyc1":"OVC","p01m":"0.00","valid":"2013-01-04 20:15","alti":"29.71","skyl1":"3200.0","vsby":"10.0","wxcodes":"M","tmpf":"32.0","station":"FSO"}
{"feel":"22.06","mslp":"-1.0","sped":"6.9","drct":"240.0","dwpf":"24.26","ice_acceretion_1hr":"M","relh":"81.81","skyc1":"OVC","p01m":"0.00","valid":"2013-01-04 20:15","alti":"29.69","skyl1":"1300.0","vsby":"1.5","wxcodes":"49","tmpf":"29.12","station":"CDA"}
...
```

# 4. Unsupervised K-Means
Assuming the producer is running, execute ```sbt run``` and select "ConsumerUnsupervised".
This should result in a ```log.txt``` file that looks something like this:
```
Normalized Humidity, Normalized Visibility, Z-Score, Prediction
([0.7626999999999999,0.1,0.710526315789474],false)
([0.7414000000000001,0.1,0.6489361702127653],false)
([0.7789,0.1,-0.20000000000000265],false)
([0.7459,0.1,1.6063829787234045],false)
([0.7804000000000001,0.1,2.999999999999997],true)
([0.7371,0.1,1.127450980392157],false)
```
Please note it may take some time to get the results, which would mean
running the producer for a couple of minutes before running the model.

# 5. (Supervised) SVM
Running the SVM is similar to running the K-Means script. To do this,
execute ```sbt run``` and select "ConsumerSupervised". This should
create an ```anomalies``` directory containing subfolders, were each
subfolder contains information (features and predictions) for each point.
Please note it may take some time to get the results, which would mean
running the producer for a couple of minutes before running the model.
