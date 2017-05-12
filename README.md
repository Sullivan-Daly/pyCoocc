# pyCoocc

## About
This script allows us to do a cooccurence analysis on our ElasticSearch tweet corpus and extract the better graded words.
We use our coocurrence on some corpus on some parts of the corpus (batchs).
We can use two types of batches in order to follow trends :
* *timebatch* : batchs are created by slicing corpus duration in equal windows
* *numberbatch* : batchs are created by slicing our corpus in equal number of tweets

## Requirements 
We need two external libraries :
* *elasticsearch* : In order to connect and request our database
* *numpy* : To use efficient data structure

We can install these library through pip, with the command pip install LIBRARY_NAME or with the build in library tracker in pycharm.

## Usage 
We simply need to feel two variables :
* *S_ES_LIMIT* : is the limit elasticsearch batch size
* *S_GRANULARITY* : is the maximum size of our batchs created by number of words 

## ToDo
* Add an option to work on words or on hashtags
* Use options to give information (database's name, etc.)
* Use sparse matrix in order to limit the RAM occupation
* Create a real class to implement the exploration and reduction of words (through the seed to the end of the corpus, batch by batch)