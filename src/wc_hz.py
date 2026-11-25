import re
from collections import Counter

import hz_worker as hz

class WordCount(hz.Worker):
    
    def map(self, x):
        # CHANGE: Use a counter so we get actual counts, not just word, 1 for everything
        for word, count in Counter(re.findall(r'\w+', x.lower())).items():
            yield (word, count)

    def reduce(self, word, counts):
        yield sum(counts)
