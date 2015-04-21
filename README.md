Search Prediction Using Spark Models
======================

Using the api from [elasticsearch-prediction](https://github.com/mahisoft/elasticsearch-prediction), this is the implementation that uses Spark as the backend to compute large scale models offline, and generates a plugin for elasticsearch for runtime evaluation of a trained scoring function.

Note that currently the only suppported spark models are linear, until the serialization and spark.ml API matures out of beta

