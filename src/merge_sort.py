"""External merge sort for line-oriented files, ie files too big for memory.

The unit of work is a whole line: only the sort key is ever decoded, and the
line itself is carried along verbatim, so a record can move from file to file
without being parsed or re-encoded.  Sorting is stable -- sorted() is, and
heapq.merge breaks ties by iterator order -- so equal keys keep their input
order.
"""

import heapq
import itertools
import operator
import os
import shutil
import tempfile

from math import sqrt, ceil
from tqdm import tqdm
from typing import Any, Callable, Iterable

KeyFn = Callable[[str], Any]

# CHANGE: kept multiplying by 2 until it felt too big
DEFAULT_BATCH_SIZE = 32768
# a merge costs only O(log fanin) per record, so a wide fan-in is nearly free
# and saves whole read/write passes over the data; the cap is open file
# descriptors, one per run in the final merge.
DEFAULT_FANIN = 64

# itemgetter rather than a lambda: these run once per record per pass, and the
# C-level version skips a Python frame each time.
_BY_KEY = operator.itemgetter(0)
_LINE = operator.itemgetter(1)


def line_prefix_key(line: str) -> str:
    """Default key: the part of the line before the first tab, undecoded.

    This is the right key for the tab-separated key/value lines of
    reduce_util.kv_to_line, where the encoded keys sort like the keys do.
    """
    return line[:line.index('\t')]


def _keyed_lines_from(input_path, key: KeyFn) -> Iterable:
    """Iterator over (key, line) for the lines of a file.

    A missing final newline is supplied here, so a line is always safe to
    write back out as-is.
    """
    with open(input_path) as fp:
        for line in fp:
            yield key(line), line if line[-1] == '\n' else line + '\n'


class _SpillFileIndex:
    """One generation of sorted runs: a temp directory, deleted as a unit.

    Runs hold the (key, line) records of _keyed_lines_from, and are iterated in
    the order they were added -- which is what keeps the sort stable, since
    heapq.merge breaks ties by iterator order.  Use it as a context manager, or
    call close() yourself, to remove the directory.
    """
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix='spill-')
        self.names = []

    def add(self, records: Iterable):
        """Write one run.  The records must ALREADY be in key order: either
        sorted() in memory, or heapq.merge()d off of earlier runs.
        """
        name = os.path.join(self.dir, f'run-{len(self.names):05d}.tsv')
        with open(name, 'w') as fp:
            fp.writelines(map(_LINE, records))
        self.names.append(name)

    def __iter__(self) -> Iterable:
        return iter(self.names)

    def __len__(self) -> int:
        return len(self.names)

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self.names = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def merge_sorted_files(input_paths, output_path, key: KeyFn = line_prefix_key):
    """Merge files whose lines are already sorted by key into output_path.
    """
    merged = heapq.merge(*[_keyed_lines_from(p, key) for p in input_paths],
                         key=_BY_KEY)
    with open(output_path, 'w') as fp:
        fp.writelines(map(_LINE, merged))


# CHANGE: add together count records that have the same key after sorting (performs a count-specific reduce operation locally, before the actual distributed reduce stage)
def _combined_count_lines(records: Iterable):
    """Collapse adjacent ``key<TAB>integer`` records.

    ``records`` must already be sorted by key.  Keeping this count-specific
    operation separate from the ordinary merge preserves merge_sort_file's
    stable, line-oriented contract.
    """
    for encoded_key, group in itertools.groupby(records, key=_BY_KEY):
        total = 0
        for _, line in group:
            _, separator, value = line.rstrip('\n').partition('\t')
            if not separator:
                raise ValueError(f'count record has no tab: {line!r}')
            total += int(value)
        yield encoded_key + '\t' + str(total) + '\n'

# CHANGE: share the sorting code while choosing whether duplicate counts are combined
def _merge_sort_file(input_path,
                     output_path,
                     key: KeyFn,
                     batch_size: int,
                     fanin: int,
                     verbose: bool,
                     combine_counts: bool):
    """Sort the lines of input_path into output_path by key(line).

    Only batch_size lines are held in memory at once: the input is spilled to
    disk as sorted runs, which are then merged fanin-at-a-time until few
    enough remain for a single final merge.  A fanin below 1 is rescaled to
    ceil(sqrt(number of runs)), ie just wide enough to finish in one pass.
    """
    def streams(run_names):
        return [_keyed_lines_from(name, key) for name in run_names]

    def progress(iterable):
        return tqdm(iterable) if verbose else iterable

    def say(*args):
        if verbose:
            print(*args)

    say('spilling...')
    runs = _SpillFileIndex()
    try:
        for batch in progress(itertools.batched(
                _keyed_lines_from(input_path, key), batch_size)):
            # sorted() is stable, so equal keys keep their input order
            runs.add(sorted(batch, key=_BY_KEY))

        # local, so a fanin of 0 rescales to each sort's own run count instead
        # of sticking at whatever the first sort happened to need
        if fanin < 1:
            # scale fanin to finish in one pass
            fanin = ceil(sqrt(len(runs)))

        while len(runs) > fanin:
            say(f'merging {len(runs)} files {len(runs)/fanin} merges')
            merged = _SpillFileIndex()
            for run_batch in progress(itertools.batched(iter(runs), fanin)):
                merged.add(heapq.merge(*streams(run_batch), key=_BY_KEY))
            # add() consumed each merge as it went, so this generation is done
            runs.close()
            runs = merged

        say(f'merging {len(runs)} files....')
        final_sort = heapq.merge(*streams(runs), key=_BY_KEY)

        # CHANGE: combine duplicates during the final merge to avoid another file pass
        with open(output_path, 'w') as fp:
            if combine_counts:
                fp.writelines(_combined_count_lines(final_sort))
            else:
                fp.writelines(map(_LINE, final_sort))
    finally:
        runs.close()

# CHANGE: keep the normal sorter and add a separate sorter just for count records
def merge_sort_file(input_path,
                    output_path,
                    key: KeyFn = line_prefix_key,
                    batch_size: int = DEFAULT_BATCH_SIZE,
                    fanin: int = DEFAULT_FANIN,
                    verbose: bool = False):
    """Sort lines by key while preserving every input record and tie order."""
    _merge_sort_file(input_path, output_path, key, batch_size, fanin, verbose,
                     combine_counts=False)

def merge_sort_and_combine_counts(input_path,
                                  output_path,
                                  key: KeyFn = line_prefix_key,
                                  batch_size: int = DEFAULT_BATCH_SIZE,
                                  fanin: int = DEFAULT_FANIN,
                                  verbose: bool = False):
    """Sort count records and sum records with equal keys during final merge.

    Input and output records use ``key<TAB>integer`` format.  At most one
    record per key is written, including when the sort requires multiple disk
    runs.  Memory use otherwise has the same bounds as merge_sort_file.
    """
    _merge_sort_file(input_path, output_path, key, batch_size, fanin, verbose,
                     combine_counts=True)
