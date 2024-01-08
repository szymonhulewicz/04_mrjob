from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper =self.mapper,
                   reducer = self.reducer
            ),
            MRStep(mapper =self.mapper_swap_kv,
                   reducer = self.reducer_sort
            )
        ]


    def mapper(self, _, line):
        client_id, _, price = line.split(',')
        yield client_id, float(price)

    def reducer (self, client_id, prices):
        total = sum(prices)

        yield client_id, total

    def mapper_swap_kv(self, client_id, prices):
        yield None, (prices, client_id)

    def reducer_sort(self, _, prices):  
        for total, client_id in sorted(prices):
            yield total, client_id       


if __name__ == '__main__':
    MRCounter.run()