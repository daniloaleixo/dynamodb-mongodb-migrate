'use strict';
const lodash = require('lodash');
const MongoDBConnector = require('./../connectors/MongoDBConnector');

class MongoDBDAO {

    constructor(tableName, databaseName, targetConnectionOptions) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.targetConnectionOptions = targetConnectionOptions;
    }

    async intertOrUpdateItems(items) {
        let dbConn = await MongoDBConnector.getConnection(this.databaseName, this.targetConnectionOptions);
        let collection = dbConn.collection(this.tableName);
        let bulkWriteReqArray = lodash.map(items, (item) => {
            return {
                insertOne: { _id: item.primaryID, ...item }
            }
        });
        return collection.bulkWrite(bulkWriteReqArray);
    }
}

module.exports = MongoDBDAO;