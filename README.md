## Project Goal
This is a project uses AWS Lambda and Comprehend to monitor cryptocurrency prices in real-time, and analyze sentiment score of each cryptocurrency on Tweeter.

## Architecture 
The architecture for the pipeline is shown below. 

![](https://user-images.githubusercontent.com/58792/55354483-bae7af80-547a-11e9-9909-a5621251065b.png)

**Description**
* CloudWatch Timer: Set to trigger producer lambda every one minutes
* DynamoDB: Store the crypto token which we want to monitor
* Producer Lambda: Read crypto token from DynamoDB once triggered by CloudWatch Timer, and save crypto analyzing tasks to SQS.
* SQS: Queuing the tasks to be executed by the consumer lambda
* Consumer Lambda: Retrieve the latest price of the token, collect latest tweets, conduct sentiment analysis. And then save the result to S3 bucket.


## Repository Structure
This repository contains the code of:
* [Producer Lambda](https://github.com/RyC37/crypto-monitor/blob/master/pricemonitor/pricemonitor/lambda_function.py)
* [Consumer Lambda](https://github.com/RyC37/crypto-monitor/blob/master/cryptosentiment/cryptosentiment/lambda_function.py)


## Reference Video:
https://www.youtube.com/watch?v=zXxdbtamoa4&feature=youtu.be
