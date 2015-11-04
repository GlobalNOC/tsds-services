# GlobalNOC TSDS Services
The `grnoc-tsds-services` package provides all of the backend functionality for the GlobalNOC TimeSeries Data Service (TSDS)
software suite.  TSDS is a system that uses a [RabbitMQ](https://www.rabbitmq.com) messaging queue and [MongoDB](https://www.mongodb.org) database to store and query time series data over time.  Here is an example showing how a full TSDS-based
deployment may look:

![TSDS Diagram](https://globalnoc.iu.edu/uploads/91/01/91018071abe575b32264779567bd1d05/tsds_flow_diagram.png "TSDS Diagram")

Some of the individual components this package provides include:

- Web Services
- Writers
- Installation/Bootstrap
- Upgrades
- Search Indexer
- Data Aggregators
- Data Expiration

For help on installing TSDS, please refer to the [Install Guide](https://github.com/GlobalNOC/tsds-services/blob/master/INSTALL.md).
