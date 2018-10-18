from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import time

class pie:
		
	def __init__(self):
		# Creating a Kafka consumer to read our Kafka topic
		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
		self.fig, self.ax = plt.subplots(1, 3, sharey=True)
		self.sentiment_count = [0,0,0] 
		self.colors = ['lightgreen', 'red', 'cyan']
		self.explode = (0.01, 0.01, 0.01)
		self.labels = ['positive', 'negative', 'neutral']
		self.positive = []
		self.negativ = []
		self.consumer.subscribe(['twitter1']) # Subscribe to a topic
		self.t_count = 0
	
	# This function updates the counters for each category and
	# also limits the text output to four tweets at a time
	def listen_and_add(self):
		i = 0
		for message in self.consumer:
			i +=1
	        
			if message.value=="1":
				self.sentiment_count[0]+=1
				if self.t_count<5:
					self.positive.append(message.key.decode('utf-8').strip())
					if len(self.positive)>4:
						self.positive.pop(0)
			elif message.value=="-1":
				self.sentiment_count[1]+=1
				if self.t_count<5:
					self.negativ.append(message.key.decode('utf-8').strip())
					if len(self.negativ)>4:
						self.negativ.pop(0)
			elif message.value=="0":
				self.sentiment_count[2]+=1
			if(i>3):
				return
	    	
	# Closes the consumer
	def close_consumer(self):
		self.consumer.close()
		print("end consumer")

	# Update the pie-chart
	def update(self,num):
		self.t_count +=1
		if self.t_count>10:
			self.t_count = 0
		self.listen_and_add() # Read messages in the Kafka topic and add to data 
		print(self.sentiment_count)
		# Clear output for next animation
		self.ax[0].clear()
		self.ax[0].axis('equal')
		self.ax[1].clear()
		self.ax[1].axis('off')
		self.ax[1].axis('equal')
		self.ax[2].clear()
		self.ax[2].axis('off')
		self.ax[2].axis('equal')
		time.sleep(0.5) # Slow down the animation in order to make the visualization easier to read
	 	str_num = "neutral " + str(self.sentiment_count[2])
		self.ax[0].pie(self.sentiment_count, explode=self.explode, labels=self.labels, colors=self.colors,
			autopct='%1.1f%%', shadow=True, startangle=140)
		self.ax[0].set_title(str_num)
		s1 = "positive " + str(self.sentiment_count[0]) +'\n'+'\n' 
		s2 = "negative " + str(self.sentiment_count[1]) +'\n'+'\n'
		for s in self.positive:
			s1 += s+ '\n'+'\n'
		for s in self.negativ:
			s2 += s+ '\n'+'\n'
		self.ax[1].text(0, 1, s1,ha='left', va='top',fontsize=12,wrap=True)
		self.ax[2].text(-1.8, -1, s2,ha='left', va='center',fontsize=12,wrap=True)

	# Run the visualisation
	def run(self):
		ani = FuncAnimation(self.fig, self.update, repeat=True)
		plt.show()

if __name__ == "__main__":
	p = pie()
	p.run()
	p.close_consumer()
