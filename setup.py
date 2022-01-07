from setuptools import setup, find_packages

setup(
    name = "pyroads",
    version = "0.1.3",
    packages = find_packages(),
    description = "For analysis of road asset data.",
    url = "https://github.com/shaan-nmb/pyroads",
    requires= ['pandas', 'numpy'],
    author = 'Shaan Ciantar'
)