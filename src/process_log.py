# Code for fansite analytics Challange

# imports 
import sys
import io
import time
import re
from datetime import datetime
from datetime import timedelta
import operator
import shlex

#  define list and dicts
answer1 = {}
answer2 = {}
answer3 = {}
answer4 = {}
blocked = []


# IN - main(); OUT - writeData()
#
def answers (line):
	# each line contains 1. host, 2. timestamp 3. request 4. bytes or '-'
	# using regex to get the data from each line
	print(line)
	temp = line.rstrip('\n').split(" ")
	host = (re.search("^\S+", line)).group(0)
	
	timestamp = (re.search("\[(.*?)\]", line)).group(1)
	# modifying for answer 3
	busiestHour = re.sub(":[0-9]+:[0-9]\w ", ":00:00 ", timestamp)
	# stripping time for answer 4
	tempTime = timestamp.split(" ")
	timestamp = time.strptime(tempTime[0], "%d/%b/%Y:%H:%M:%S")
	timestamp =  time.mktime(timestamp)

	request = (re.search("\"(.*?)\"", line))
	print(request)
	request = request.group(1).split(" ") if len(request.group(1).split(" ")) > 2 else [0,0,0]
	requestType = request[0]
	requestedResource = request[1]
	#security = request[2]
	HTTPreply = temp[-2]
	bytesSend = 0 if temp[-1] == "-" else temp[-1]
	#print(host, timestamp, requestType, requestedResource, requestedResource, HTTPreply, bytesSend) 

	answer1[host] = answer1.get(host, 0) +1
	if requestedResource != 0:
		answer2[requestedResource] = answer2.get(requestedResource, 0) + int(bytesSend)
	answer3[busiestHour] = answer3.get(busiestHour, 0) + 1
	
	# for answer 4 first we check if the reply is 401
	if HTTPreply == "401":
		# next we check if this host is already in our watchlist
		if host in answer4:
			if len(answer4[host]) == 1:
				answer4[host].append(timestamp)
			elif len(answer4[host]) == 3:
				diff = abs(answer4[host][-1] - timestamp)
				if diff <= 300:
					blocked.append(line.rstrip('\n'))
				else:
					# outside the 5 minute window removing the block
					answer4.pop(host, None)
			elif len(answer4[host]) == 2:
				secondTime = answer4[host][1]
				diff = abs(secondTime - timestamp)
				if diff < 20:
					firstTime = answer4[host][0]
					diff = abs(firstTime - timestamp)
					if diff < 20:
						answer4[host].append(timestamp)
					else:
						answer4[host] = answer4[host][-2]
				else:
					answer4.append(timestamp)
			
		else: #just add the host with the time 
			answer4[host] = [timestamp]

	if HTTPreply == "200" and host in answer4:
		if len(answer4[host]) < 3:
			answer4.pop(host, None)
		else:
			diff = abs(answer4[host][-1] - timestamp)
			if diff <= 300:
				blocked.append(line.rstrip('\n'))
			else:
				# outside the 5 minute window removing the block
				answer4.pop(host, None)

	
	

# IN - medianDegree(); OUT - output(s).txt
############################################################################
def writeData(outputPath, writeThis):
	output = open(outputPath, 'a')
	if(len(writeThis[0]) == 2):
		for i in writeThis:
			temp = str(i[0])+","+str(i[1])
			output.write(temp)
			output.write("\n")
	else:
		for i in writeThis:
			output.write(str(i))
			output.write("\n")
	output.close()
	

# IN - log.txt files; OUT - def answers()

def main():
	if len(sys.argv) != 6:
		print("Please provide the path for input file and four output files. Or execute the run.sh")
		sys.exit(1)

	IP1 = sys.argv[1]
	OP1 = sys.argv[2]
	OP2 = sys.argv[3]
	OP3 = sys.argv[4]
	OP4 = sys.argv[5]

	# to work on both windows and linux/unix systems
	try:
		output = open(OP1, 'w')
		output = open(OP2, 'w')
		output = open(OP3, 'w')
		output = open(OP4, 'w')
		output.close()
	except:
		os.chdir("../")
		output = open(OP1, 'w')
		output = open(OP2, 'w')
		output = open(OP3, 'w')
		output = open(OP4, 'w')
		output.close()
		
	# open the input file and send lines to answers()
	with io.open(IP1, 'r', encoding = "ascii", errors = "ignore") as fileToRead:
	    for line in fileToRead:
	    	answers(line)
	fileToRead.close()

	sorted_answer1 = sorted(answer1.items(), key=operator.itemgetter(1), reverse = True)
	sorted_answer2 = sorted(answer2.items(), key=operator.itemgetter(1), reverse = True)
	sorted_answer3 = sorted(answer3.items(), key=operator.itemgetter(1), reverse = True)
	

	print("Top 10 most active host/IP addresses are: \n", sorted_answer1[:10], "\n")
	print("Top 10 resources that consume the most bandwidth are: \n", ([x[0] for x in sorted_answer2[:10]]), "\n")
	print("Top 10 busiest hours are: \n", sorted_answer3[:10], "\n")
	print("Blocked accesses are: \n", blocked[:10], "\n")
	print("Total number of blocked accesses are: ", len(blocked), "\n")

	totalRequests = sum(answer5.values())
	print("Total number of requests made to the server are: ", totalRequests)
	

	# writing the results to the output files
	writeData(OP1, sorted_answer1[:10])
	writeData(OP2, ([x[0] for x in sorted_answer2[:10]]))
	writeData(OP3, sorted_answer3[:10])
	writeData(OP4, blocked)

if __name__ == '__main__':
	main()
