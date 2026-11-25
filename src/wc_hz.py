import re
from collections import Counter

import hz_worker as hz

class WordCount(hz.Worker):
    
    def map(self, x):
        # CHANGE: Use a counter so we get actual counts, not just word, 1 for everything
        for word, count in Counter(re.findall(r'\w+', x.lower())).items():
            yield (word, count)

    #CHANGE: count words across multiple lines so that the mapper writes fewer records (instead of restarting the Counter for every line)
    MAPPER_COMBINE_MAX_ENTRIES = 250_000 # prevents the Counter from using too much of the worker's 2GB RAM
    combine_counts = True

    def map_inputs(self, lines):
        """Combine words across lines, flushing (restarting) at a bounded vocabulary size."""
        counts = Counter()
        for line in lines:
            counts.update(re.findall(r'\w+', line.lower()))
            if len(counts) >= self.MAPPER_COMBINE_MAX_ENTRIES:
                yield from counts.items()
                counts.clear()
        yield from counts.items()

    def reduce(self, word, counts):
        yield sum(counts)
