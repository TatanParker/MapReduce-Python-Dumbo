import unittest
import wordcountMR as wc
from dumbo.mapredtest import MapReduceDriver

class WordCountTests(unittest.TestCase):
	def testWordcountDriver(self):
		driver = MapReduceDriver(wc.wordcountMapper, wc.wordcountReducer)
		input_kvs = [(1, "Solo se que no se nada")]
		output_kvs = [("Solo", 1), ("se", 2), ("que", 1), ("no", 1), ("nada", 1)]
		driver.with_input(input_kvs)
		driver.with_output(sorted(output_kvs))
		driver.run()
		
if __name__ == "__main__":
	suite = unittest.TestLoader().loadTestsFromTestCase(WordCountTests)
	unittest.TextTestRunner(verbosity=2).run(suite)