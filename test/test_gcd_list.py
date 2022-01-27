from pyroads.reshape._functions._gcd_list import gcd_list
import numpy as np

def test_gcd_list_1():
	tests = [
		[1, [2, 3, 4, 5, 6, 7, 8, 9, 10]],
		[2, [2, 4, 6, 8, 10]],
		[3, [3, 6, 9,3*50]],
	]
	for result, items in tests:
		assert result == gcd_list(items)

	# confirm the result is the same as numpy.gcd.reduce()
	for _result, items in tests:
		assert np.gcd.reduce(items) == gcd_list(items)