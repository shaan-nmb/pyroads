import numpy as np


def most(x):
	if len(x.mode()):
		return x.mode()[0]
	else:
		return np.nan


def first(x):
	return x.iloc[0]


def last(x):
	return x.iloc[-1]


def q75(x):
	return x.quantile(0.75)


def q90(x):
	return x.quantile(0.90)


def q95(x):
	return x.quantile(0.95)