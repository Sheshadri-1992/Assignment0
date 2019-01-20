#!/usr/bin/python

import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as colors
import numpy as np

fname="/home/swamiji/phd/sem2/ssds/Assignment0/plots/values.txt"
q1_keys="buckets.txt"
q1_values="values.txt"

q2_keys = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
q2_values= "pairHashTagCounts.txt"

q4_keys = [1,5,10,25,100]
q4_values = [2.25, 5.1, 8.33, 20, 102]

#number of executors
q5_keys = ["2","3","4","5"]
q5_values = [5.3,3.4,3.2,2.25]

#number of cores
q6_keys = ["1","2","3"]
q6_values = [3.9, 4.25,7]

#amount of memory
q7_keys = ["2GB","4GB","6GB", "8GB"]
q7_values = [3,3.5,4.1,2.25]

def plotGraph(content):

	plt.subplot(111)
	fig, ax1 = plt.subplots()	

	X = []

	with open("buckets.txt") as f:
		X = f.readlines()

	X = [int(a.strip()) for a in X]

	# X = q2_keys
	# X = q4_keys	

	Y = []

	with open("values.txt") as f:
		Y = f.readlines()

	Y = [float(a.strip()) for a in Y]
	
	# Y = q4_values

	plt.bar(q6_keys , q6_values, align='center', width = 0.3,color = 'blue')	
	# plt.scatter(X,Y,s=[13],color='red')	
	# plt.plot(X,Y,linewidth=2,color='blue',marker="^",markerfacecolor='red')	

	#Q1
	# plt.title('Frequency distribution of hashtags per tweet ', weight='bold' ,fontsize=13)	
	# plt.xlabel("Number of Hashtags per Tweet ",fontsize =14)	
	# plt.ylabel("Users (in 10 million) ",fontsize=14)	   

	#Q2
	# plt.title('Frequency of co-occuring hashtags ', weight='bold' ,fontsize=14)	
	# plt.xlabel("Top 100 co-occuring hashtags ",fontsize =14)	
	# plt.ylabel("Number of occurrences",fontsize=14)	    

	#Q4
	# plt.title('Time for execution of FreqTag.java', weight='bold' ,fontsize=13)	
	# plt.xlabel("Percentage of Data (~1TB) ",fontsize =14)	
	# plt.ylabel("Time for execution (in mins) ",fontsize=14)	   

	#Q5
	# plt.title('Different number of executors (FreqTag.java) ', weight='bold' ,fontsize=13)	
	# plt.xlabel("Number of executors ",fontsize =14)	
	# plt.ylabel("Time for execution (in mins) ",fontsize=14)	   

	#Q6
	plt.title('Different number of cores (FreqTag.java) ', weight='bold' ,fontsize=13)	
	plt.xlabel("Number of cores ",fontsize =14)	
	plt.ylabel("Time for execution (in mins) ",fontsize=14)	   

	#Q7
	# plt.title('Different amount of memory (FreqTag.java) ', weight='bold' ,fontsize=13)	
	# plt.xlabel("Amount of Memory for a single executor",fontsize =14)	
	# plt.ylabel("Time for execution (in mins) ",fontsize=14)	  

	# plt.xticks(X,X,fontsize=8, rotation=30,weight='bold')
	# plt.xticks([0,10,20,30,40,50,60,70,80,90,100],weight='bold')
	plt.xticks(weight='bold')
	#plt.xticks([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100],weight='bold')
	plt.yticks(weight='bold')
	
	
	plt.rc('axes',titlesize=18)	
	plt.rc('legend', fontsize=18)    # legend fontsize
	plt.rc('figure', titlesize=18)	
	# plt.grid(b=None, which='major', axis='both')
	# plt.grid(which='major', linestyle='', linewidth='0.25', color='gray')

	
	plt.tight_layout()

	plt.savefig('q_extra_2.png')


with open(fname) as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content] 

print content
plotGraph(content)