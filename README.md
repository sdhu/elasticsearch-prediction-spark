Search Prediction Using Spark Models
======================

Using the api from [elasticsearch-prediction](https://github.com/mahisoft/elasticsearch-prediction), this is the implementation that uses Spark as the backend to compute large scale models offline, and generates a plugin for elasticsearch for runtime evaluation of a trained scoring function.

*This is highly experimental code as a proof of concept, so there are many many areas of improvements, and bugs. Use for fun only*

Note that currently the only suppported spark models are linear, until the serialization and spark.ml API matures out of beta

Some references:
- [scripting-scores](http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/script-score.html)
- [modules-scripting](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-scripting.html)

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark and [elasticsearch-prediction](https://github.com/mahisoft/elasticsearch-prediction).
