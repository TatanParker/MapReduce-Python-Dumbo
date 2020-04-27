import sys


def wordcountMapper(k, value):
    """Input: key: offset, value: content
       Output: key: word, value: 1"""
    wordslist = value.split(" ")
    for word in wordslist:
        yield word, 1


def wordcountReducer(key, values):
    """Input: key: word, value: (1, 1, 1)
       Output: key: word, value: sum(1s)"""
    yield key, sum(values)


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text"), ]
    o1 = job.additer(wordcountMapper, wordcountReducer, combiner=wordcountReducer, opts=opts)


if __name__ == "__main__":
    from dumbo import main

    main(runner)
