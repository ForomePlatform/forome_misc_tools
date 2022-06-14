import setuptools

with open("forome_tools/README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "forome_tools",
    version = "0.1.10",
    py_modules = ['forome_tools'],
    author = "Sergey Trifonov, with colleagues",
    author_email = "trf@ya.ru",
    description = "Miscellaneous tools used in Forome Platform",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/ForomePlatform/forome_misc_tool",
    packages = setuptools.find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent"]
)
