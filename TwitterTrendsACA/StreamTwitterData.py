from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import os
import json
import time
import sys
import boto3

# Authentication details.
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

GEOBOX_WORLD = [-180,-90,180,90]
GEOBOX_USA = [-165.6,12.6,-52.7,66.4]


# Output directory to hold text files with tweets

outputDir = "outputDir"

## End of Settings###

class FileDumperListener(StreamListener):

	def __init__(self,filepath):
		super(FileDumperListener,self).__init__(self)
		self.basePath=filepath
		os.system("mkdir %s"%(filepath))

		d=datetime.today()
		self.filename = "%i-%02d-%02d_1.txt"%(d.year,d.month,d.day)		
		self.fh = open(self.basePath + "/" + self.filename,"a")#open for appending just in case
		
		self.tweetCount=0
		self.errorCount=0
		self.limitCount=0
		self.last=datetime.now()
	
	#This function gets called every time a new tweet is received on the stream
	def on_data(self, data):
            decoded = json.loads(data.strip())
            text_data = decoded.get('text')
            try:
                if text_data is not None:
                    self.fh.write(text_data.encode('ascii','ignore')+'\n')
                    self.tweetCount += 1
                    self.status()
                    return True
            except Exception as e:
                print("Error on_data: %s" % str(e))
                time.sleep(5)
            return True	
	def close(self):
		try:
			self.fh.close()
		except:
			pass
	
	#Rotate the log file if needed.
	#Warning: Check for log rotation only occurs when a tweet is received and not more than once every five minutes.
	#		  This means the log file could have tweets from a neighboring period (especially for sparse streams)
	def rotateFiles(self):
		d=datetime.today()
		filenow = "%i-%02d-%02d_1.txt"%(d.year,d.month,d.day)
		if (self.filename!=filenow):
			print("%s - Rotating log file. Old: %s New: %s"%(datetime.now(),self.filename,filenow))
			try:
				self.fh.close()
			except:
				#Log/Email it
				pass
			self.filename=filenow
			self.fh = open(self.basePath + "/" + self.filename,"a")

	def on_error(self, statusCode):
		print("%s - ERROR with status code %s"%(datetime.now(),statusCode))
		self.errorCount+=1
	
	def on_timeout(self):
		raise TimeoutException()
	
	def on_limit(self, track):
		print("%s - LIMIT message recieved %s"%(datetime.now(),track))
		self.limitCount+=1
	
	def status(self):
		now=datetime.now()
		if (now-self.last).total_seconds()>300:
			print("%s - %i tweets, %i limits, %i errors in previous five minutes."%(now,self.tweetCount,self.limitCount,self.errorCount))
			self.tweetCount=0
			self.limitCount=0
			self.errorCount=0
			self.last=now
			self.rotateFiles()#Check if file rotation is needed
		

class TimeoutException(Exception):
	pass

if __name__ == '__main__':
	while True:
		try:
			#Create the listener
			listener = FileDumperListener(outputDir)
			auth = OAuthHandler(consumer_key, consumer_secret)
			auth.set_access_token(access_token, access_token_secret)

			#Connect to the Twitter stream
			stream = Stream(auth, listener)
			#stream.filter(locations=[-0.530, 51.322, 0.231, 51.707])#Tweets from London
			stream.filter(locations=GEOBOX_USA, languages=['en'])

		except KeyboardInterrupt:
			#User pressed ctrl+c or cmd+c -- get ready to exit the program
			print("%s - KeyboardInterrupt caught. Closing stream and exiting."%datetime.now())
			listener.close()
			stream.disconnect()
			break
		except TimeoutException:
			#Timeout error, network problems? reconnect.
			print("%s - Timeout exception caught. Closing stream and reopening."%datetime.now())
			try:
				listener.close()
				stream.disconnect()
			except:
				pass
			continue
		except Exception as e:
			#Anything else
			try:
				info = str(e)
				sys.stderr.write("%s - Unexpected exception. %s\n"%(datetime.now(),info))
			except:
				pass
			time.sleep(240)

time.sleep(10)
s3 = boto3.resource('s3')
for tmp in os.listdir(outputDir):
    if tmp.endswith(".txt"):
        s3.Object('twittertweets', 'input'+"/"+tmp).put(Body=open(outputDir+"/"+tmp, 'rb'))


