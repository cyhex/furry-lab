from distutils.core import setup
setup(
    name="furry_lab_puppy",
    packages=["furry_lab_puppy"],
    version="1.0.0",
    description="generate fire clusters from FIRMS data",
    install_requires=[
      "scikit-learn",
      "pyspark"
    ],
)
