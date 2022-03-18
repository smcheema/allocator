from model_dataclass import *
import json
from types import SimpleNamespace



if __name__=='__main__':

    reader = SimulationReader("test", "2022-03-15_19.32.28")
    step = reader.getSimulationStep(0)

    print(step)