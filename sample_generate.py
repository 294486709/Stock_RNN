import sqlite3
import numpy as np
import os
import shutil
from random import shuffle
from tqdm import tqdm

TESTING_RATIO = 0.2
SAMPLE_PERIOD = 6
POS_RATIO = 0.03


def initialize():
	print("Clear previous samples")
	try:
		shutil.rmtree('Samples')
	except FileNotFoundError:
		pass
	os.mkdir('Samples')
	print('New folder generated')


def get_class():
	stock_class = os.listdir('Class')
	return stock_class


def shuffle_sample(X_total, Y_total):
	combined = list(zip(X_total,Y_total))
	shuffle(combined)
	X_total[:], Y_total[:] = zip(*combined)
	return X_total, Y_total



def Stock_sample_gen(s_code, c):
	COMMAND = "SELECT * FROM {}".format(s_code)
	try:
		data = c.execute(COMMAND).fetchall()
	except sqlite3.OperationalError:
		return [], []
	X_total = []
	Y_total = []
	if len(data) < 50 + SAMPLE_PERIOD:
		return
	for i in range(40, len(data) - SAMPLE_PERIOD):
		X = []
		for j in range(i,i+SAMPLE_PERIOD):
			X.append(data[j][8:])
		X = np.array(X)
		temp_ratio = data[i+SAMPLE_PERIOD][7]
		Y = np.zeros(3)
		if temp_ratio >= POS_RATIO:
			Y[2] = 1
		elif temp_ratio <= -POS_RATIO:
			Y[0] = 1
		else:
			Y[1] = 1
		X_total.append(X)
		Y_total.append(Y)
	X_total, Y_total = shuffle_sample(X_total, Y_total)
	return X_total, Y_total


def sample_gen(file, c):
	f = open('Class/{}'.format(file), 'r')
	S_code = f.readlines()
	X_total = []
	Y_total = []
	for i in range(len(S_code)):
		S_code[i] = S_code[i][:-1].replace(".","_").lower()
	for i in tqdm(range(len(S_code))):
		X_temp, Y_temp = Stock_sample_gen(S_code[i], c)
		X_total.extend(X_temp)
		Y_total.extend(Y_temp)
	np.savez_compressed('Samples/{}/Samples.npz'.format(file[:-4]), X=np.array(X_total), Y=np.array(Y_total))


def main():
	connection = sqlite3.connect('{}.db'.format('Stock'))
	c = connection.cursor()
	initialize()
	files = get_class()
	counter = 1
	for file in files:
		os.mkdir('Samples/{}'.format(file[:-4]))
		print('Current task:{} , progress {}/{}'.format(file[:-4],counter,len(files)))
		sample_gen(file, c)
		counter += 1


if __name__ == '__main__':
	main()
