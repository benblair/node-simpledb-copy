// node-simpledb-copy
// copies simpledb from one account / region to another
// use with caution! Doesn't check anything before overwriting. Actually, just don't use this :)

var _ = require('underscore');
var awssum = require('awssum');
var amazon = awssum.load('amazon/amazon');
var SimpleDB = awssum.load('amazon/simpledb').SimpleDB;

var env             = process.env;
var accessKeyId     = env.ACCESS_KEY_ID;
var secretAccessKey = env.SECRET_ACCESS_KEY;

var READ_REGION = amazon.US_EAST_1;
var WRITE_REGION = amazon.US_WEST_2;
var SELECT_LIMIT = '25';     // few at a time to ensure lower error rate on write
var CONSISTENT_READ = false; // faster
var EXCLUDE_DOMAINS = [
    'activity', 'auth', 'companies', 'inventory-test', 
    'price-histograms-test', 'price-observations', 'price-observations-test'
];
// since we already copied these before crashing...
EXCLUDE_DOMAINS = EXCLUDE_DOMAINS.concat([
    'inventory',
    'manufacturers',
    'member_plans',
    'order_history',
    'orders',
    'payments',
    // 'price-histograms' // didn't actually finish, just skipping for priority's sake
    'products',
    'trades',
    'transactions',
    'uploads',
    'users'
]);

var readSdb = new SimpleDB({
    'accessKeyId'     : accessKeyId,
    'secretAccessKey' : secretAccessKey,
    'region'          : READ_REGION
});

var writeSdb = new SimpleDB({
    'accessKeyId'     : accessKeyId,
    'secretAccessKey' : secretAccessKey,
    'region'          : WRITE_REGION
});

var readBoxUsage = 0;
var writeBoxUsage = 0;
var domainsCopied = 0;
var recordsCopied = 0;

var skippedBatches = [];

function reportResults() {
    console.log('Total read box usage: ' + readBoxUsage);
    console.log('Total write box usage: ' + writeBoxUsage);
    console.log('Domains copied: ' + domainsCopied);
    console.log('Records copied: ' + recordsCopied);
    console.log('Following is a dump of all items that were skipped due to errors.');
    console.log(JSON.stringify({ skippedItems: skippedBatches }));
}

function copyDomains(nextToken, callback) {

    if (!callback && _.isFunction(nextToken)) {
        callback = nextToken;
        nextToken = null;
    }

    var requestParams = { MaxNumberOfDomains: 1 };
    if (nextToken) {
        requestParams.NextToken = nextToken;
    }

    readSdb.ListDomains(requestParams, function(err, data) {
        if (err) {
            callback(err);
            return;
        }

        var response = data.Body.ListDomainsResponse;
        readBoxUsage += parseFloat(response.ResponseMetadata.BoxUsage);
        var results = response.ListDomainsResult;
        nextToken = results.NextToken;
        var domain = results.DomainName;

        if (!domain) {
            callback();
            return;
        }

        if (EXCLUDE_DOMAINS.indexOf(domain) >= 0) {

            console.log('Skipping excluded domain "' + domain + '"');

            copyDomains(nextToken, callback);
            return;

        }

        readSdb.DomainMetadata({ DomainName: domain }, function(err, data) {

            if (err) {
                callback(err);
                return;
            }

            var recordCount = data.Body.DomainMetadataResponse.DomainMetadataResult.ItemCount;

            console.log('Copying domain "' + domain + '" with ' + recordCount + ' records...');

            ensureWriteDomain(domain, function(err) {

                if (err) {
                    callback(err);
                    return;
                }

                domainsCopied += 1;

                copyRecords(domain, 0, recordCount, null, function(err, result) {

                    if (err) {
                        callback(err);
                        return;
                    }

                    if (nextToken) {
                        copyDomains(nextToken, callback);
                    } else {
                        callback();
                    }
                });
            });
        });

    });
}

function ensureWriteDomain(domain, callback) {
    writeSdb.DomainMetadata({ DomainName: domain }, function(err, data) {
        if (err) {

            if (err.Body.Response.Errors.Error.Code === 'NoSuchDomain') {

                writeSdb.CreateDomain({ DomainName: domain }, function(err, data) {
                    if (err) {
                        callback(err);
                        return;
                    }

                    console.log('Created domain "' + domain + '" in write region "' + WRITE_REGION + '"');

                    // once got a 'domain does not exist' error right after creating domain, so maybe just 
                    // need to give it some time...

                    setTimeout(function() {
                        callback();
                    }, 10000);
                });

                return;
            }
            callback(err);
            return;
        }

        callback();

    });
}

function copyRecords(domain, copiedRecords, recordCount, nextToken, callback) {

    if (!callback && _.isFunction(nextToken)) {
        callback = nextToken;
        nextToken = null;
    }

    var requestParams = { SelectExpression: 'SELECT * FROM `' + domain + '` LIMIT ' + SELECT_LIMIT };
    if (nextToken) {
        requestParams.NextToken = nextToken;
    }
    requestParams.ConsistentRead = CONSISTENT_READ;

    readSdb.Select(requestParams, function(err, data) {
        if (err) {
            callback(err);
            return;
        }

        var response = data.Body.SelectResponse;
        readBoxUsage += parseFloat(response.ResponseMetadata.BoxUsage);
        var results = response.SelectResult;
        var items = results.Item;
        if (!items || items.length === 0) {
            callback();
            return;
        }
        var count = items.length;
        nextToken = results.NextToken;

        writeRecords(domain, items, function(err, data) {

            if (err) {
                callback(err);
                return;
            }

            console.log(
                'Copied a batch of ' + count + ' records to "' + domain + '" in region "' + WRITE_REGION + '" ' + 
                '[' + Math.round(100 * (copiedRecords + count) / recordCount) + '%]');

            if (nextToken) {
                copyRecords(domain, copiedRecords + count, recordCount, nextToken, callback);
                return;
            }

            callback(err, data);

        });

    });

}

function prepareForBatchPut(selectedItems) {
    var keys = [];
    var attributeNames = [];
    var attributeValues = [];
    var attributeReplaces = [];

    _.each(selectedItems, function(item) {
        keys.push(item.Name);
        attributeNames.push(_.pluck(item.Attribute, 'Name'));
        attributeValues.push(_.pluck(item.Attribute, 'Value'));
        attributeReplaces.push(_.map(_.range(item.Attribute.length), function() { return false }));
    });

    return {
        keys: keys,
        attributeNames: attributeNames,
        attributeValues: attributeValues,
        attributeReplaces: attributeReplaces
    };
}

function writeRecords(domain, items, callback) {

    var records = prepareForBatchPut(items);

    var requestParams = {
        DomainName      : domain,
        ItemName        : records.keys,
        AttributeName   : records.attributeNames,
        AttributeValue  : records.attributeValues,
        AttributeReplace: records.attributeReplaces
    };

    writeSdb.BatchPutAttributes(requestParams, function(err, data) {
        if (err) {

            if (err.Body.Response.Errors.Error.Code === 'SignatureDoesNotMatch') {
                // Pin down the errant record(s) and skip them
                // TODO: write the object into ./skipped-items.json

                if (items.length === 1) {
                    // found the culprit
                    console.log('Skipping an item due to repeated write errors of the "SignatureDoesNotMatch" variety. All skipped items will be shown at the end of the copy.');
                    skippedBatches.push({ domain: domain, items: items });
                    callback();
                    return;
                }

                var firstHalf = items.slice(0, Math.floor(items.length / 2));
                var secondHalf = items.slice(Math.floor(items.length / 2));

                console.log('Encountered a "SignatureDoesNotMatch" error. This sometimes indicates we\'re hitting SimpleDB too hard. Backing off for 5 seconds...');

                setTimeout(function() {
                    writeRecords(domain, firstHalf, function(err) {
                        if (err) {
                            callback(err);
                            return;
                        }

                        writeRecords(domain, secondHalf, function(err) {
                            if (err) {
                                callback(err);
                                return;
                            }

                            callback();
                        });
                    });
                }, 5000);

                return;
            }

            console.log('Error writing records to write domain "' + domain + '"');
            callback(err);
            return;
        }

        var response = data.Body.BatchPutAttributesResponse;
        writeBoxUsage += parseFloat(response.ResponseMetadata.BoxUsage);

        callback();
    });
}

copyDomains(function(err, results) {
    if (err) {
        if (err.Body && err.Body.Response.Errors) {
            err = err.Body.Response.Errors;
        }
        console.log(err);
        reportResults(results);
        return;
    }

    reportResults(results);
});