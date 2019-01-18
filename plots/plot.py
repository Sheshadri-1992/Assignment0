#!/usr/bin/python

import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as colors
import numpy as np

fname="/home/swamiji/phd/sem2/ssds/Assignment0/plots/buckets.txt"


def plotGraph(content):

	plt.subplot(111)
	fig, ax1 = plt.subplots()	

	X = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47]
	Y = [174584965,26061816,7962843,3563603,1875683,1038112,560681,306767,225191,144416,97322,68856,48926,32184,18763,9015,4954,2609,1386,1393,1048,687,391,369,188,131,23,12,40,15,4,5,2,1,0,1,0,0,0,0,0,0,0,1,0,0,0,1]
	

	plt.bar(X , Y, width = 1,color = 'skyblue')	

	plt.xlabel("Buckets (hashtags per tweet) ",fontsize =18)
	plt.xticks(X,X,fontsize=12, rotation=45)
	plt.ylabel("Number of Users",fontsize=18)	    
	plt.title('Frequency of Users vs Number of Users ', fontsize=16)	
	plt.rc('axes',titlesize=18)
	plt.rc('axes',labelsize=18)	
	plt.rc('legend', fontsize=18)    # legend fontsize
	plt.rc('figure', titlesize=18)	
	plt.ylim([0,174584965])
	plt.tight_layout()
	
	#save the
	plt.savefig('graph.png')


with open(fname) as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content] 

print content
plotGraph(content)