import os

def main():
	effective = open('effective.txt','r')
	effective = effective.readlines()
	for i in range(len(effective)):
		effective[i] = effective[i][:-1]
	files = os.listdir('Stock_data')
	for file in files:
		if file[:-4] not in effective:
			os.remove('Stock_data/{}'.format(file))

if __name__ == '__main__':
	main()