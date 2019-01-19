#!/usr/bin/python

import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as colors
import numpy as np

fname="/home/swamiji/phd/sem2/ssds/Assignment0/plots/values.txt"


def plotGraph(content):

	plt.subplot(111)
	fig, ax1 = plt.subplots()	

	X = []

	with open("buckets.txt") as f:
		X = f.readlines()

	X = [a.strip() for a in X]

	Y = []

	with open("values.txt") as f:
		Y = f.readlines()

	Y = [float(a.strip()) for a in Y]
	
	

	# plt.bar(X , Y, width = 1,color = 'skyblue')	
	plt.plot(X,Y)	
	plt.xlabel("Buckets (hashtags per tweet) ",fontsize =14)

	plt.xticks(X,X,fontsize=8, rotation=30,weight='bold')
	plt.yticks(weight='bold')
	plt.ylabel("Number of Users",fontsize=14)	    
	plt.title('Frequency of Users vs Number of Users ', fontsize=14)	
	plt.rc('axes',titlesize=18)	
	plt.rc('legend', fontsize=18)    # legend fontsize
	plt.rc('figure', titlesize=18)	

	#save the
	plt.savefig('graph.png')


with open(fname) as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content] 

print content
plotGraph(content)