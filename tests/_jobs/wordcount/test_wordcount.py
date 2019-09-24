from pyspark import SparkContext
from jobs.wordcount import analyze

def test_wordcount_analyze():
    sc = SparkContext.getOrCreate()
    sc.addPyFile("dist/jobs.zip")
    sc.addPyFile("dist/libs.zip")
    sc.addPyFile("dist/main.py")
    result = analyze(sc)
    assert len(result) == 327
    assert result[:6] == [('ut', 17), ('eu', 16), ('vel', 14), ('nec', 14), ('vitae', 12), ('quis', 12)]
