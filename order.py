from mrjob.job import MRJob
import re

WORD_REGEX = re.compile(r"[\w]+")

class MRCounter(MRJob):


    def mapper(self, _, line):
        client_id, _, price = line.split(',')
        yield client_id, float(price)

    def reducer (self, client_id, prices):
        total = sum(prices)

        yield client_id, total


if __name__ == '__main__':
    MRCounter.run()