import keras
import numpy as np
import os

HIDDEN_SIZE = 128

def RNN(X_train, Y_train, X_test, Y_test):
	model = keras.models.Sequential()
	# model.add(keras.layers.Flatten())
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	# model.add(keras.layers.Dense(1000))
	#
	# model.add(keras.layers.Dense(3))
	model.add(keras.layers.LSTM(HIDDEN_SIZE, return_sequences=True))
	# model.add(keras.layers.Dropout(0.2))

	model.add(keras.layers.LSTM(HIDDEN_SIZE, return_sequences=True))
	# model.add(keras.layers.Dropout(0.2))

	model.add(keras.layers.LSTM(HIDDEN_SIZE, return_sequences=True))
	# model.add(keras.layers.Dropout(0.2))

	model.add(keras.layers.LSTM(HIDDEN_SIZE, return_sequences=True))
	# model.add(keras.layers.Dropout(0.2))

	model.add(keras.layers.Flatten())
	# # model.add(keras.layers.Dense(256, activation='relu'))
	# # model.add(keras.layers.Dense(128, activation='relu'))
	# # model.add(keras.layers.Dense(64, activation='relu'))
	model.add(keras.layers.Dense(3, activation='softmax'))

	model.compile(optimizer='Adam', loss = 'categorical_crossentropy', metrics=['accuracy'])
	c = model.fit(X_train, Y_train, epochs=5, batch_size=128, verbose=1, validation_split=0.2)
	# model = keras.models.load_model('1.h5')
	model.save('1.h5')

	res = model.evaluate(x=X_test, y=Y_test, batch_size=128)
	# print(res)
	# print('\nTesting loss: {}, acc: {}\n'.format(loss, acc))
	pass



def data_fetch(sample):
	data = np.load('Samples/{}/Samples.npz'.format(sample))
	X_train = data['X_train']
	Y_train = data['Y_train']
	X_test = data['X_test']
	Y_test = data['Y_test']
	# return list(X_train), list(Y_train), list(X_test), list(Y_test)
	return X_train, Y_train, X_test, Y_test


def main():
	samples = os.listdir('Samples')
	if len(samples) < 1:
		print('Sample Not Found!')
		return
	for sample in samples:
		X_train, Y_train, X_test, Y_test = data_fetch(sample)
		RNN(X_train, Y_train, X_test, Y_test)
	pass


if __name__ == '__main__':
	main()