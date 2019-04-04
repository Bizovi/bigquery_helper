from setuptools import setup, find_packages

setup(name='bigquery_helper',
      version='0.1',
      description='Bigquery utility functions',
      url='http://github.com/bizovi/bigquery_helper',
      author='Bizovi Mihai',
      author_email='bizovim@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          "google-cloud", "pandas", "google-auth"
      ],
      include_package_data=True,
      zip_safe=False)
