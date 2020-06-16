import pandas as pd
import pyreadr as pr

r_data = pr.read_r('r_data/mydata_af.0.r')

print(len(r_data))
