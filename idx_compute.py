import numpy as np
import talib as tl
import sklearn.preprocessing
import sqlite3
import os


def transaction_bldr(sql):
	global sql_transaction
	sql_transaction.append(sql)
	if len(sql_transaction) > 5000:
		c.execute('BEGIN TRANSACTION')
		for s in sql_transaction:
			try:
				c.execute(s)
			except:
				pass
		connection.commit()
		sql_transaction = []


def normal(data):
	for i in range(len(data)):
		if not np.isnan(data[i]):
			start = i
			break
	front = data[:start]
	end = data[start:]
	end = np.reshape(end, (-1, 1))
	front = np.reshape(front,(-1,1))
	end = sklearn.preprocessing.normalize(end, axis=0, norm='l2')
	data = np.vstack((front,end))
	data = np.reshape(data, (1, -1))

	return data[0]


def fatch_data(S_code, num, c):
	data = {}
	open = np.zeros(num)
	high = np.zeros(num)
	low = np.zeros(num)
	close = np.zeros(num)
	volumn = np.zeros(num)
	for i in range(num-1, -1, -1):
		data_line = c.execute('SELECT * FROM {} where IDX = {}'.format(S_code, i)).fetchone()
		open[i] = data_line[2]
		high[i] = data_line[3]
		low[i] = data_line[4]
		close[i] = data_line[5]
		volumn[i] = data_line[6]
	data['open'] = open
	data['high'] = high
	data['low'] = low
	data['close'] = close
	data['volume'] = volumn
	return data


def WILLR(S_code, current_idx, data, startidx):
	real = tl.WILLR(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET WILLR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ULTOSC(S_code, current_idx, data, startidx):
	real = tl.ULTOSC(data['high'], data['low'], data['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ULTOSC = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def TRIX(S_code, current_idx, data, startidx):
	real = tl.TRIX(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET TRIX = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def STOCHF(S_code, current_idx, data, startidx):
	K,D = tl.STOCHF(data['high'], data['low'], data['close'], fastk_period=5, fastd_period=3, fastd_matype=0)
	K = normal(K)
	D = normal(D)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET STOCHFK = {} WHERE IDX = {}'.format(S_code, K[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET STOCHFD = {} WHERE IDX = {}'.format(S_code, D[i], i)
		transaction_bldr(COMMAND)


def STOCH(S_code, current_idx, data, startidx):
	K,D = tl.STOCH(data['high'], data['low'], data['close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
	J = []
	for i in range(startidx):
		J.append(np.nan)
	for i in range(startidx, current_idx):
		J.append(3*D[i] - 2*K[i])
	J = np.array(J)
	K = normal(K)
	D = normal(D)
	J = normal(J)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET STOCHK = {} WHERE IDX = {}'.format(S_code, K[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET STOCHD = {} WHERE IDX = {}'.format(S_code, D[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET STOCHJ = {} WHERE IDX = {}'.format(S_code, J[i], i)
		transaction_bldr(COMMAND)


def MACDFIX(S_code, current_idx, data, startidx):
	macd, macdsignal, macdhist = tl.MACDFIX(data['close'], signalperiod=9)
	macd = normal(macd)
	macdsignal = normal(macdsignal)
	macdhist = normal(macdhist)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MACDFIX = {} WHERE IDX = {}'.format(S_code, macd[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDFIXS = {} WHERE IDX = {}'.format(S_code, macdsignal[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDFIXH = {} WHERE IDX = {}'.format(S_code, macdhist[i], i)
		transaction_bldr(COMMAND)


def MACDEXT(S_code, current_idx, data, startidx):
	macd, macdsignal, macdhist = tl.MACDEXT(data['close'], fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
	macd = normal(macd)
	macdsignal = normal(macdsignal)
	macdhist = normal(macdhist)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MACDEXT = {} WHERE IDX = {}'.format(S_code, macd[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDEXTS = {} WHERE IDX = {}'.format(S_code, macdsignal[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDEXTH = {} WHERE IDX = {}'.format(S_code, macdhist[i], i)
		transaction_bldr(COMMAND)


def MACD(S_code, current_idx, data, startidx):
	macd, macdsignal, macdhist = tl.MACD(data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
	macd = normal(macd)
	macdsignal = normal(macdsignal)
	macdhist = normal(macdhist)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MACD = {} WHERE IDX = {}'.format(S_code, macd[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDS = {} WHERE IDX = {}'.format(S_code, macdsignal[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET MACDH = {} WHERE IDX = {}'.format(S_code, macdhist[i], i)
		transaction_bldr(COMMAND)


def BBANS(S_code, current_idx, data, startidx):
	upperband, middleband, lowerband = tl.BBANDS(data['close'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
	upperband = normal(upperband)
	middleband = normal(middleband)
	lowerband = normal(lowerband)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET BBANDSU = {} WHERE IDX = {}'.format(S_code, upperband[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET BBANDSM = {} WHERE IDX = {}'.format(S_code, middleband[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET BBANDSL = {} WHERE IDX = {}'.format(S_code, lowerband[i], i)
		transaction_bldr(COMMAND)


def ROCP(S_code, current_idx, data, startidx):
	real = tl.ROCP(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ROCP = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ROCR(S_code, current_idx, data, startidx):
	real = tl.ROCR(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ROCR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ROCR100(S_code, current_idx, data, startidx):
	real = tl.ROCR100(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ROCR100 = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ROC(S_code, current_idx, data, startidx):
	real = tl.ROC(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ROC = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def PPO(S_code, current_idx, data, startidx):
	real = tl.PPO(data['close'], fastperiod=12, slowperiod=26, matype=0)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET PPO = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MOM(S_code, current_idx, data, startidx):
	real = tl.MOM(data['close'], timeperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MOM = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def CCI(S_code, current_idx, data, startidx):
	real = tl.CCI(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET CCI = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def BOP(S_code, current_idx, data, startidx):
	real = tl.BOP(data['open'], data['high'], data['low'], data['close'])
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET BOP = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def AROONOSC(S_code, current_idx, data, startidx):
	real = tl.AROONOSC(data['high'],data['low'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ARRONOSC = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def AROON(S_code, current_idx, data, startidx):
	aroondown, aroonup = tl.AROON(data['high'], data['low'], timeperiod=14)
	aroonup = normal(aroonup)
	aroondown = normal(aroondown)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ARROND = {} WHERE IDX = {}'.format(S_code, aroondown[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET ARRONU = {} WHERE IDX = {}'.format(S_code, aroonup[i], i)
		transaction_bldr(COMMAND)


def APO(S_code, current_idx, data, startidx):
	real = tl.APO(data['close'], fastperiod=12, slowperiod=26, matype=0)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET APO = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ADX(S_code, current_idx, data, startidx):
	real = tl.ADX(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ADX = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def CMO(S_code, current_idx, data, startidx):
	real = tl.CMO(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET CMO = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def DX(S_code, current_idx, data, startidx):
	real = tl.DX(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET DX = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MINUS_DI(S_code, current_idx, data, startidx):
	real = tl.MINUS_DI(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MINUSDI = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MINUS_DM(S_code, current_idx, data, startidx):
	real = tl.MINUS_DM(data['high'], data['low'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MINUSDM = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def PLUS_DI(S_code, current_idx, data, startidx):
	real = tl.PLUS_DI(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET PLUSDI = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def PLUS_DM(S_code, current_idx, data, startidx):
	real = tl.PLUS_DM(data['high'], data['low'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET PLUSDM = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def RSI(S_code, current_idx, data, startidx):
	real = tl.RSI(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET RSI = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def STOCHRSI(S_code, current_idx, data, startidx):
	K,D = tl.STOCHRSI(data['close'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
	K = normal(K)
	D = normal(D)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET STOCHRSIK = {} WHERE IDX = {}'.format(S_code, K[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET STOCHRSID = {} WHERE IDX = {}'.format(S_code, D[i], i)
		transaction_bldr(COMMAND)


def MFI(S_code, current_idx, data, startidx):
	real = tl.MFI(data['high'], data['low'], data['close'], data['volume'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MFI = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ADXR(S_code, current_idx, data, startidx):
	real = tl.ADXR(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ADXR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def HT(S_code, current_idx, data, startidx):
	real = tl.HT_TRENDLINE(data['close'])
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET HT = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def KAMA(S_code, current_idx, data, startidx):
	real = tl.KAMA(data['close'], timeperiod=30)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET KAMA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MA(S_code, current_idx, data, startidx):
	real = tl.MA(data['close'], timeperiod=30, matype=0)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MAMA(S_code, current_idx, data, startidx):
	mama, fama = tl.MAMA(data['close'])
	mama = normal(mama)
	fama = normal(fama)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MAMA = {} WHERE IDX = {}'.format(S_code, mama[i], i)
		transaction_bldr(COMMAND)
		COMMAND = 'UPDATE {} SET FAMA = {} WHERE IDX = {}'.format(S_code, fama[i], i)
		transaction_bldr(COMMAND)


def MIDPOINT(S_code, current_idx, data, startidx):
	real = tl.MIDPOINT(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MIDPOINT = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def MIDPRICE(S_code, current_idx, data, startidx):
	real = tl.MIDPRICE(data['high'], data['low'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET MIDPRICE = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def SMA(S_code, current_idx, data, startidx):
	real = tl.SMA(data['close'], timeperiod=30)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET SMA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def T3(S_code, current_idx, data, startidx):
	real = tl.T3(data['close'], timeperiod=5, vfactor=0)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET T3 = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def TRIMA(S_code, current_idx, data, startidx):
	real = tl.TRIMA(data['close'], timeperiod=30)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET TRIMA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def WMA(S_code, current_idx, data, startidx):
	real = tl.WMA(data['close'], timeperiod=30)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET WMA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def AD(S_code, current_idx, data, startidx):
	real = tl.AD(data['high'], data['low'], data['close'], data['volume'])
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET AD = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ADOSC(S_code, current_idx, data, startidx):
	real = tl.ADOSC(data['high'], data['low'], data['close'], data['volume'], fastperiod=3, slowperiod=10)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ADOSC = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def OBV(S_code, current_idx, data, startidx):
	real = tl.OBV(data['close'], data['volume'])
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET OBV = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def ATR(S_code, current_idx, data, startidx):
	real = tl.ATR(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET ATR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def BETA(S_code, current_idx, data, startidx):
	real = tl.BETA(data['high'], data['low'], timeperiod=5)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET BETA = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def CORREL(S_code, current_idx, data, startidx):
	real = tl.CORREL(data['high'], data['low'], timeperiod=30)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET CORREL = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def LINEARREG(S_code, current_idx, data, startidx):
	real = tl.LINEARREG(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET LINEARREG = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def LINEARREGANGLE(S_code, current_idx, data, startidx):
	real = tl.LINEARREG_ANGLE(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET LINEARREGANGLE = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def LINEARREGINTERCEPT(S_code, current_idx, data, startidx):
	real = tl.LINEARREG_INTERCEPT(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET LINEARREGINTERCEPT = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def LINEARREGSLOPE(S_code, current_idx, data, startidx):
	real = tl.LINEARREG_SLOPE(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET LINEARREGSLOPE = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def STDDEV(S_code, current_idx, data, startidx):
	real = tl.STDDEV(data['close'], timeperiod=5, nbdev=1)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET STDDEV = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def TSF(S_code, current_idx, data, startidx):
	real = tl.TSF(data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET TSF = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def VAR(S_code, current_idx, data, startidx):
	real = tl.VAR(data['close'], timeperiod=5, nbdev=1)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET VAR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def NATR(S_code, current_idx, data, startidx):
	real = tl.NATR(data['high'], data['low'], data['close'], timeperiod=14)
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET NATR = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def TRANGE(S_code, current_idx, data, startidx):
	real = tl.TRANGE(data['high'], data['low'], data['close'])
	real = normal(real)
	for i in range(startidx, current_idx):
		COMMAND = 'UPDATE {} SET TRANGE = {} WHERE IDX = {}'.format(S_code, real[i], i)
		transaction_bldr(COMMAND)


def PATTERN(S_code, current_idx, data, startidx, pattern_list):
	flag = []
	flag.append(tl.CDL2CROWS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3BLACKCROWS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3INSIDE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3LINESTRIKE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3OUTSIDE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3STARSINSOUTH(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDL3WHITESOLDIERS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLABANDONEDBABY(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLADVANCEBLOCK(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLBELTHOLD(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLBREAKAWAY(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLCLOSINGMARUBOZU(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLCONCEALBABYSWALL(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLCOUNTERATTACK(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLDARKCLOUDCOVER(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLDOJI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLDOJISTAR(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLDRAGONFLYDOJI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLENGULFING(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLEVENINGDOJISTAR(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLEVENINGSTAR(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLGAPSIDESIDEWHITE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLGRAVESTONEDOJI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHAMMER(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHANGINGMAN(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHARAMI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHARAMICROSS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHIGHWAVE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHIKKAKE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHIKKAKEMOD(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLHOMINGPIGEON(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLIDENTICAL3CROWS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLINNECK(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLINVERTEDHAMMER(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLKICKING(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLKICKINGBYLENGTH(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLLADDERBOTTOM(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLLONGLEGGEDDOJI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLLONGLINE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLMARUBOZU(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLMATCHINGLOW(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLMATHOLD(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLMORNINGDOJISTAR(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLMORNINGSTAR(data['open'], data['high'], data['low'], data['close'], penetration=0))
	flag.append(tl.CDLONNECK(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLPIERCING(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLRICKSHAWMAN(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLRISEFALL3METHODS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSEPARATINGLINES(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSHOOTINGSTAR(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSHORTLINE(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSPINNINGTOP(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSTALLEDPATTERN(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLSTICKSANDWICH(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLTAKURI(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLTASUKIGAP(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLTHRUSTING(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLTRISTAR(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLUNIQUE3RIVER(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLUPSIDEGAP2CROWS(data['open'], data['high'], data['low'], data['close']))
	flag.append(tl.CDLXSIDEGAP3METHODS(data['open'], data['high'], data['low'], data['close']))
	for i in range(startidx, current_idx):
		for j in range(len(pattern_list)):
			COMMAND = 'UPDATE {} SET {} = {} WHERE IDX = {}'.format(S_code, pattern_list[j] ,flag[j][i]/20000+0.005, i)
			transaction_bldr(COMMAND)


def update_index(S_code, pattern_list):
	S_code = S_code.replace('.','_')
	S_code = S_code.lower()
	try:
		current_idx = c.execute('SELECT * FROM {} ORDER BY IDX DESC LIMIT 1'.format(S_code)).fetchone()[0]
		current_idx += 1
	except sqlite3.OperationalError:
		return 
	if current_idx < 40:
		return
	for i in range(current_idx-1,-1,-1):
		if i == 40:
			startidx = 40
		flag = c.execute('SELECT * FROM {} WHERE IDX = {}'.format(S_code, i)).fetchone()
		if flag[len(flag)-1] is not None:
			startidx = i
			break

	data = fatch_data(S_code, current_idx, c)
	BBANS(S_code, current_idx, data, startidx)
	HT(S_code, current_idx, data, startidx)
	KAMA(S_code, current_idx, data, startidx)
	MA(S_code, current_idx, data, startidx)
	MAMA(S_code, current_idx, data, startidx)
	MIDPOINT(S_code, current_idx, data, startidx)
	MIDPRICE(S_code, current_idx, data, startidx)
	SMA(S_code, current_idx, data, startidx)
	T3(S_code, current_idx, data, startidx)
	TRIMA(S_code, current_idx, data, startidx)
	WMA(S_code, current_idx, data, startidx)
	ADX(S_code, current_idx, data, startidx)
	ADXR(S_code, current_idx, data, startidx)
	DX(S_code, current_idx, data, startidx)
	CMO(S_code, current_idx, data, startidx)
	STOCHRSI(S_code, current_idx, data, startidx)
	RSI(S_code, current_idx, data, startidx)
	PLUS_DI(S_code, current_idx, data, startidx)
	PLUS_DM(S_code, current_idx, data, startidx)
	MINUS_DI(S_code, current_idx, data, startidx)
	MINUS_DM(S_code, current_idx, data, startidx)
	MFI(S_code, current_idx, data, startidx)
	APO(S_code, current_idx, data, startidx)
	AROON(S_code, current_idx, data, startidx)
	AROONOSC(S_code, current_idx, data, startidx)
	BOP(S_code, current_idx, data, startidx)
	CCI(S_code, current_idx, data, startidx)
	MACD(S_code, current_idx, data, startidx)
	MACDEXT(S_code, current_idx, data, startidx)
	MACDFIX(S_code, current_idx, data, startidx)
	MOM(S_code, current_idx, data, startidx)
	PPO(S_code, current_idx, data, startidx)
	ROC(S_code, current_idx, data, startidx)
	ROCP(S_code, current_idx, data, startidx)
	ROCR(S_code, current_idx, data, startidx)
	ROCR100(S_code, current_idx, data, startidx)
	STOCH(S_code, current_idx, data, startidx)
	STOCHF(S_code, current_idx, data, startidx)
	TRIX(S_code, current_idx, data, startidx)
	ULTOSC(S_code, current_idx, data, startidx)
	WILLR(S_code, current_idx, data, startidx)
	AD(S_code, current_idx, data, startidx)
	ADOSC(S_code, current_idx, data, startidx)
	OBV(S_code, current_idx, data, startidx)
	ATR(S_code, current_idx, data, startidx)
	NATR(S_code, current_idx, data, startidx)
	TRANGE(S_code, current_idx, data, startidx)
	PATTERN(S_code, current_idx, data, startidx, pattern_list)
	BETA(S_code, current_idx, data, startidx)
	CORREL(S_code, current_idx, data, startidx)
	LINEARREG(S_code, current_idx, data, startidx)
	LINEARREGANGLE(S_code, current_idx, data, startidx)
	LINEARREGINTERCEPT(S_code, current_idx, data, startidx)
	LINEARREGSLOPE(S_code, current_idx, data, startidx)
	STDDEV(S_code, current_idx, data, startidx)
	TSF(S_code, current_idx, data, startidx)
	VAR(S_code, current_idx, data, startidx)






def main():
	files = os.listdir(os.getcwd()+'/Stock_data')
	pattern_list = []
	ff = open('templete')
	ff = ff.readlines()
	for i in ff:
		pattern_list.append(i[:-1])

	for i in range(len(files)):
		file = files[i]
		file_name = file[:-4]
		update_index(file_name, pattern_list)
		print('{} over {} completed! Progress:{}%'.format(i+1,len(files),((i+1)/len(files))))

	c.execute('BEGIN TRANSACTION')

	# update the data
	for s in sql_transaction:
		try:
			c.execute(s)
		except:
			pass
	connection.commit()

sql_transaction = []
connection = sqlite3.connect("{}.db".format('Stock'))
c = connection.cursor()
main()