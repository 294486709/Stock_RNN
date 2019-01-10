import sqlite3
import os
import tqdm
import numpy as np

START_DATE = 20100101

def update_raw(c, S_code, connection):
	data = np.loadtxt('Stock_data/{}.txt'.format(S_code), dtype=np.str)
	S_code = S_code.replace('.', '_')
	S_code = S_code.lower()
	idx = 0
	for i in range(data.shape[0]):
		current_line = data[i]
		if len(current_line) < 7:
			continue
		if int(current_line[6]) < START_DATE:
			continue
		COMMAND = 'INSERT INTO {}(IDX, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, RATE) VALUES(?,?,?,?,?,?,?,?)'.format(S_code)
		value = []
		value.append(idx)
		value.append(int(current_line[6]))
		value.append(float(current_line[1]))
		value.append(float(current_line[2]))
		value.append(float(current_line[3]))
		value.append(float(current_line[4]))
		value.append(int(current_line[5]))
		try:
			value.append((float(current_line[4])-float(current_line[1]))/float(current_line[1]))
		except ZeroDivisionError:
			value.append(0)
		try:
			c.execute(COMMAND,tuple(value))
			idx += 1
		except:
			pass
	try:
		connection.commit()
	except:
		pass




def main():
	connection = sqlite3.connect('{}.db'.format('Stock'))
	c = connection.cursor()
	files = os.listdir(os.getcwd()+'/Stock_data')
	for i in tqdm.tqdm(range(len(files))):
		file = files[i]
		file_name = file[:-4]
		update_raw(c, file_name, connection)



if __name__ == '__main__':
	main()
