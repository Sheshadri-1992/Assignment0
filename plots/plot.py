#!/usr/bin/python

import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as colors

fname="/home/swamiji/phd/sem2/ssds/Assignment0/plots/buckets.txt"


def plotGraph(content):

	plt.subplot(111)
	fig, ax1 = plt.subplots()	

	X = np.arange(94) #how many databases you want? 10,20...150 totally is 14
	
	plt.bar(X , content, width = 0.3,color = 'skyblue')	
	plt.autoscale(tight=True)
	# plt.xticks(X,spill_db_size,fontsize=19, rotation=45)
	# plt.yticks(fontsize=19)
	plt.xlabel("Hashtags per tweet ",fontsize = 22)
	plt.ylabel("Number of Users",fontsize=22)	    
	blue_patch = mpatches.Patch(color='blue_patch', label='Number of users')
	plt.legend(handles=[red_patch],loc='best')
	plt.title('Number of hashtags per tweet ', fontsize=22)	
	plt.rc('axes',titlesize=22)
	plt.rc('axes',labelsize=22)	
	plt.rc('legend', fontsize=22)    # legend fontsize
	plt.rc('figure', titlesize=22)	
	plt.ylim([0,16])
	plt.tight_layout()
	
	#save the
	plt.savefig('graph.png')


with open(fname) as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content] 

print content
plotGraph(content)