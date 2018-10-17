from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
class pie:
	
	
	def __init__(self):
		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
		self.fig, self.ax = plt.subplots(1, 3, sharey=True)
		self.sentiment_count = [0,0,0] 
		self.colors = ['green', 'red', 'cyan']
		self.explode = (0.01, 0.01, 0.01)
		self.labels = ['posetiv', 'negativ', 'neutral']
		self.posetiv = []
		self.negativ = []
		self.consumer.subscribe(['twitter1'])
		for message in self.consumer:
			print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, str(message.key),
                                          str(message.value)))
			
	#subscribe to a topic change to twitter
	#reads 10 medelanden och updaterar count pos neg ne
	def listen_and_add(self):
		i = 0
		for message in self.consumer:
			i +=1
	        
			if message.value=="1":
				self.sentiment_count[0]+=1
				self.posetiv.append(str(message.key))
				if len(self.posetiv)>4:
					self.posetiv.pop(0)
			elif message.value=="-1":
				self.sentiment_count[1]+=1
				self.negativ.append(str(message.key))
				if len(self.negativ)>4:
					self.negativ.pop(0)
			elif message.value=="0":
				self.sentiment_count[2]+=1
			#if(i>10):
			#	return
	    	
	#closes consumer
	def close_consumer(self):
		self.consumer.close()
		print("end consumer")






	#updaterar pie charten
	def update(self,num):
		#read all kafka and add to data 
		self.listen_and_add()
		print(self.sentiment_count)
		self.ax[0].clear()
		self.ax[0].axis('equal')
		self.ax[1].clear()
		self.ax[1].axis('off')
		self.ax[1].axis('equal')
		self.ax[2].clear()
		self.ax[2].axis('off')
		self.ax[2].axis('equal')

	 	str_num = str(num)
		self.ax[0].pie(self.sentiment_count, explode=self.explode, labels=self.labels, colors=self.colors,
			autopct='%1.1f%%', shadow=True, startangle=140)
		self.ax[0].set_title(str_num)
		s1 = "posetiv " + str(self.sentiment_count[0]) +'\n'
		s2 = "negativ " + str(self.sentiment_count[1]) +'\n'
		for s in self.posetiv:
			s1 += s+ '\n'
		for s in self.negativ:
			s2 += s+ '\n'
		self.ax[1].text(0, 1, s1,ha='left', va='top',fontsize=12)
		self.ax[2].text(0, 1, s2,ha='left', va='top',fontsize=12)

	#run the visualisation
	def run(self):
		ani = FuncAnimation(self.fig, self.update, repeat=True)
		plt.show()

if __name__ == "__main__":
	p = pie()
	p.run()
	p.close_consumer()
