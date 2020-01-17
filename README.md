# Squid

Experimental "in memory sql querier via `Toucan connectors`"

### Rational:
`ToucanConnector` allows to connect to a wide variety of system: mysql, 
facebook_insights, dataiku...

Once your connectors configured, you want to query these data 
(coming from different system) in the same language.

Here I propose to simply load the output DataFrame into a sql db.
But it could be anything else.


### How to run the example:
(Run on python 3.7.4)
`pip install -r requirements.txt` then: `python -m main example_configuration.json`


### Main requirements: 
- `ToucanConnector`: https://github.com/ToucanToco/toucan-connectors/tree/master/toucan_connectors
- `pandas`
- `sql_alchemy`

NB:
- Not tested (yet)