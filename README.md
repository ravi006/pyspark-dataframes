Sonobi Test Seed Project
========================

Introduction
----
TBW

1. **Creating Virtual Environment and Adding Dependencies**

     # [Install Virtualenv] (https://virtualenv.pypa.io/en/stable/installation/)
     
     `sudo pip install virtualenv`
     
     # Creating virtual env
     
     `virtualenv -p <path o python 2.7.12> --setuptools env`
     
     # Entering in virtual env
     
     `source env/bin/activate`
     
     # Adding Packages
     
     `pip install -r requirements.txt`
     
2. **Dependencies**

     # Dev Environment

     `Hadoop-2.*`
     `Hive-*`
     `Spark-2.*`
     `python-2.7.12`

3. **Create Egg**
    Run this command everytime we make a change in any of the python files
    `python setup.py bdist_egg`

4. **Spark run job**
    `spark-submit  --jars spark-csv_2.10-1.4.0.jar,commons-csv-1.4.jar test/src/runner/spark_etl/test_runner.py  --master local[1]`
    `OR able build egg then use this command`
    `spark-submit  --jars spark-csv_2.10-1.4.0.jar,commons-csv-1.4.jar --py-files dist/test_seed-1.0-py2.7.egg test/src/runner/spark_etl/test_runner.py  --master local[1]`
