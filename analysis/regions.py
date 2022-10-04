import collections

from fetch import fetch

count = 0
other = 0

names = collections.Counter()

for (_source, _id, dataset) in fetch():
    region = dataset["region"]

    if region:
        count += 1

        if "Other" in region:
            other += 1

            names[region["Other"]] += 1

print(f"{count} regions, {other} ({100 * other / count:.1f}) unknown")

for (name, count) in names.most_common():
    print(f"{name}: {count}")
