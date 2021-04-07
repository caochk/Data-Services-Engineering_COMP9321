import numpy as np
import pandas as pd
import sys

def process_data(oldDf):
    


def part1(origTrainDf, origValidDf):
    process_data(origTrainDf)





if __name__ == "__main__":
    trainingPath, validationPath = sys.argv[1], sys.argv[2]
    origTrainDf = pd.read_csv(trainingPath)
    origValidDf = pd.read_csv(validationPath)
    part1(origTrainDf, origValidDf)
    # part2(trainPath, validationPath)