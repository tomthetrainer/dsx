from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
import os

## CONSTANTS

APP_NAME = "My Spark Application"

conf = SparkConf().setAppName(APP_NAME)
sc = SparkContext(conf=conf)

##getDirectory() - returns root directory for distributed files
##get(filename) - returns absolute path to the file

#with open(SparkFiles.get('iris.txt')) as test_file:
#    print(test_file.read())

print(SparkFiles.getRootDirectory())

files = os.listdir(SparkFiles.getRootDirectory())
for file in files:
      #do some stuff
    print("######")
    print(file)

    #print SparkFiles.get()

##with open(SparkFiles.get('test.yml')) as test_file:
##    logging.info(test_file.read())

#files = os.listdir(os.curdir)
#for file in files:
#      #do some stuff
#      print(file)
