

from setuptools import setup, find_packages


setup(name="tap-monday",
      version="0.0.1",
      description="Singer.io tap for extracting data from Monday API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_monday"],
      install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "backoff==2.2.1"
      ],
      extras_require={'dev': ['pylint', 'pytest', 'ipdb']},
      entry_points="""
          [console_scripts]
          tap-monday=tap_monday:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_monday": ["schemas/*.json"],
      },
      include_package_data=True,
)

