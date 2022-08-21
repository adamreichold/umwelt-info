import collections

from fetch import fetch


histogram = collections.Counter()

for (_source, _id, dataset) in fetch():
    histogram[len(dataset["resources"])] += 1

total = histogram.total()
cumsum = 0

for (resources, count) in histogram.most_common():
    cumsum += count

    print(f"{resources}: {100 * cumsum/ total:.1f} %")
