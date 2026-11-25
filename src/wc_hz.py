import re

import hz_worker as hz

class WordCount(hz.Worker):
    combine_counts = True
    
    def map(self, x):
        for word in re.findall(r'\w+', x.lower()):
            yield (word, 1)

    def reduce(self, word, counts):
        yield sum(counts)
