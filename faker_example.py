from faker import Faker
import pandas as pd
import numpy as np

fake = Faker()

profs = []
for _ in range(100):
    profs.append(fake.profile())

feature1 = np.random.rand(100)
feature2 = np.random.rand(100)

data = pd.DataFrame(profs)
data['feature1'] = feature1
data['feature2'] = feature2
