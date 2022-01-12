from setuptools import setup, find_packages

setup(
    name = "pyroads",
    version = "0.1.30",
    packages = find_packages(),
    description = "For analysis of road asset data.",
    url = "https://github.com/shaan-nmb/pyroads",
    install_requires= ['pandas', 'numpy'],
    author = 'Shaan Ciantar'
)