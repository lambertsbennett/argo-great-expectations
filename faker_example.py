from faker import Faker
import pandas as pd

fake = Faker()

profs = []
for _ in range(100):
    profs.append(fake.profile())

data = pd.DataFrame(profs)