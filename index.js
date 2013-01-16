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
var writeDomainTag  = env.WRITE_DOMAIN_TAG || '';
var readRegion      = env.READ_REGION || amazon.US_EAST_1;
var writeRegion     = env.WRITE_REGION || amazon.US_WEST_2;
var lastTimestamp   = env.LAST_TIMESTAMP || '';

var deleteCopyDomainPriorToCopy = env.DELETE_DOMAIN_PRIOR_TO_COPY || false;

var SELECT_LIMIT = '25';     // few at a time to ensure lower error rate on write
var CONSISTENT_READ = false; // faster
var EXCLUDE_DOMAINS = [
    'inventory-test', 
    'price-histograms-test', 'price-observations-test'
];

var INCLUDE_DOMAINS = [
    'auth', 'manufacturers', 'inventory', 'member_plans', 'order_history', 
    'orders', 'payments', 'products', 'trades', 'transactions', 'uploads',
    'users', 'watchlists', 'price-observations', 'activity'
];

var COPY_NEWEST_ONLY_DOMAINS = [
    'activity'
];

var readSdb = new SimpleDB({
    'accessKeyId'     : accessKeyId,
    'secretAccessKey' : secretAccessKey,
    'region'          : readRegion
});

var writeSdb = new SimpleDB({
    'accessKeyId'     : accessKeyId,
    'secretAccessKey' : secretAccessKey,
    'region'          : writeRegion
});

var readBoxUsage = 0;
var writeBoxUsage = 0;
var domainsCopied = 0;
var recordsCopied = 0;

var skippedBatches = [];
var processedDomains = {};

var retryWithBackoff = function(obj, f, maxTries) {
    return function() {
        // make then invoke the invokeWithTries
        var args = Array.prototype.slice.call(arguments);
        callback = args.slice(-1)[0];
        (function invokeWithTry(tries) {
            var wrappedCallback =  function() {
                var cbArgs = Array.prototype.slice.call(arguments);
                var err = cbArgs[0];
                if (err && err.Body.Response.Errors.Error.Code === 'ServiceUnavailable') {
                    if (tries > maxTries) { 
                        console.log('Max tries reached, stopping retry');
                        return callback(err);
                    }
                    console.log('Retrying...');
                    setTimeout(function() {
                        invokeWithTry(tries + 1);
                    }, 4 ^ tries * 100); 
                    // ^ Back off exponentially per http://aws.amazon.com/articles/Amazon-SimpleDB/1394
                }
                else {
                    callback.apply(null, cbArgs);
                }
            }
            args[args.length - 1] = wrappedCallback;
            f.apply(obj, args);
        })(1);
    }
}

function reportResults() {
    console.log('Total read box usage: ' + readBoxUsage);
    console.log('Total write box usage: ' + writeBoxUsage);
    console.log('Domains copied: ' + domainsCopied);
    console.log('Records copied: ' + recordsCopied);
    console.log('Following is a dump of all items that were skipped due to errors.');
    console.log(JSON.stringify({ skippedItems: skippedBatches }));
}

function copyDomains(nextToken, callback) {
    if (!writeDomainTag && readRegion === writeRegion) {
        console.error('Can not copy from "' + readRegion + '" to "' + writeRegion + '" as no WRITE_DOMAIN_TAG specified.');
        return;
    }
    if (!callback && _.isFunction(nextToken)) {
        callback = nextToken;
        nextToken = null;
    }

    var requestParams = { MaxNumberOfDomains: 1 };
    if (nextToken) {
        requestParams.NextToken = nextToken;
    }

    retryWithBackoff(readSdb, readSdb.ListDomains, 5)(requestParams, function(err, data) {
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

        if (processedDomains[domain]) {
            console.log('Seen this domain before, exiting');
            callback();
            return;
        }

        if (EXCLUDE_DOMAINS.indexOf(domain) >= 0) {

            console.log('Skipping excluded domain "' + domain + '"');

            copyDomains(nextToken, callback);
            return;

        }

        if (INCLUDE_DOMAINS.length > 0 && INCLUDE_DOMAINS.indexOf(domain) < 0) {

            console.log('Skipping non included domain "' + domain + '"');

            copyDomains(nextToken, callback);
            return;
        }

        processedDomains[domain] = true;

        var writeDomain = domain + (writeDomainTag ? '-' + writeDomainTag : '');

        if (writeDomainTag && writeDomain === domain) {
            // Don't copy when we already have a domain copy in this region
            console.log('Skipping domain that is a copy "' + domain + '"');

            copyDomains(nextToken, callback);
            return;
        }

        retryWithBackoff(readSdb, readSdb.DomainMetadata, 5)({ DomainName: domain }, function(err, data) {

            if (err) {
                callback(err);
                return;
            }

            var recordCount = data.Body.DomainMetadataResponse.DomainMetadataResult.ItemCount;
            var copyNewestOnly = COPY_NEWEST_ONLY_DOMAINS.indexOf(domain) >= 0;

            console.log('Copying domain "' + domain + '" with ' + (!copyNewestOnly ? recordCount + ' records...' : 'data starting at ' + lastTimestamp));


            ensureWriteDomain(writeDomain, copyNewestOnly, function(err) {

                if (err) {
                    callback(err);
                    return;
                }

                domainsCopied += 1;

                copyRecords(domain, writeDomain, 0, recordCount, copyNewestOnly, lastTimestamp, null, function(err, result) {

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

function ensureWriteDomain(domain, copyNewestOnly, callback) {
    retryWithBackoff(writeSdb, writeSdb.DomainMetadata, 5)({ DomainName: domain }, function(err, data) {
        if (err) {

            if (err.Body.Response.Errors.Error.Code === 'NoSuchDomain') {

                retryWithBackoff(writeSdb, writeSdb.CreateDomain, 5)({ DomainName: domain }, function(err, data) {
                    if (err) {
                        callback(err);
                        return;
                    }

                    console.log('Created domain "' + domain + '" in write region "' + writeRegion + '"');

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
        else if (deleteCopyDomainPriorToCopy && !copyNewestOnly) {
            retryWithBackoff(writeSdb, writeSdb.DeleteDomain, 5)({ DomainName: domain }, function(err, data) {
                if (err) {
                    console.log('Unable to delete domain "' + domain + '" in write region "' + writeRegion + '"');
                    callback(err);
                    return;
                }

                console.log('Deleted domain "' + domain + '" in write region "' + writeRegion + '" prior to copy');

                setTimeout(function() {
                    ensureWriteDomain(domain, callback);
                }, 10000);

                // Don't call the callback since the recursive call to ensureWriteDomain will do so.
            });
        }
        else
        {
            callback();
        }
    });
}

function copyRecords(readDomain, writeDomain, copiedRecords, recordCount, copyNewestOnly, lastTimestamp, nextToken, callback) {
    
    var lastTime = new Date(lastTimestamp);
    if (copyNewestOnly && !lastTime.valueOf()) {
        console.warn('Unable to copy to "' + writeDomain + '".  Latest timestamp: "' + lastTimestamp +'" does not appear to be a date');
        return;
    }

    if (!callback && _.isFunction(nextToken)) {
        callback = nextToken;
        nextToken = null;
    }

    var whereStatement = '';
    if (copyNewestOnly) {
        whereStatement = " WHERE timestamp > '" + lastTime.toISOString() + "' AND timestamp LIKE '20%'";
    }

    var requestParams = { SelectExpression: 'SELECT * FROM `' + readDomain + '`' + whereStatement + ' LIMIT ' + SELECT_LIMIT };
    if (nextToken) {
        requestParams.NextToken = nextToken;
    }
    requestParams.ConsistentRead = CONSISTENT_READ;

    retryWithBackoff(readSdb, readSdb.Select, 5)(requestParams, function(err, data) {
        if (err) {
            callback(err);                
            return;
        }

        var response = data.Body.SelectResponse;
        readBoxUsage += parseFloat(response.ResponseMetadata.BoxUsage);
        var results = response.SelectResult;
        var items = results.Item;
        if (items && !_.isArray(items))
        {
            items = [items];
        }
        if (!items || items.length === 0) {
            callback();
            return;
        }
        var count = items.length;
        nextToken = results.NextToken;

        writeRecords(writeDomain, items, function(err, data) {

            if (err) {
                callback(err);
                return;
            }

            console.log(
                'Copied a batch of ' + count + ' records to "' + writeDomain + '" in region "' + writeRegion + '" ' + 
                (!copyNewestOnly ? '[' + Math.round(100 * (copiedRecords + count) / recordCount) + '%]' : ''));

            recordsCopied += count;

            if (nextToken) {
                copyRecords(readDomain, writeDomain, copiedRecords + count, recordCount, copyNewestOnly, lastTimestamp, nextToken, callback);
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

    var emptyAttributeList = _.map(selectedItems, function(item) {
        return !_.every(item.Attribute, function(val) { return _.isEmpty(val.Value)});
    });
    var needsAttributeAdded = !_.reduce(emptyAttributeList, function(memo, val) { return memo && val});
    _.each(selectedItems, function(item) {
        keys.push(item.Name);
        var attribute = _.filter(item.Attribute, function(val) { return !_.isEmpty(val.Value)});
        var names = _.pluck(attribute, 'Name');
        if (needsAttributeAdded) {
            names = _.union(names, ['itemName']);
        }
        attributeNames.push(names);
        var values = _.pluck(attribute, 'Value');
        if (needsAttributeAdded) {
            values = _.union(values, item.Name);
        }
        attributeValues.push(values);
        attributeReplaces.push(_.map(_.range(values.length), function() { return false }));
    });
    return {
        keys: keys,
        attributeNames: attributeNames,
        attributeValues: attributeValues,
        attributeReplaces: attributeReplaces
    };
}

function writeRecords(domain, items, callback) {
    if (!_.isArray(items))
    {
        items = [items];
    }
    var records = prepareForBatchPut(items);

    var requestParams = {
        DomainName      : domain,
        ItemName        : records.keys,
        AttributeName   : records.attributeNames,
        AttributeValue  : records.attributeValues,
        AttributeReplace: records.attributeReplaces
    };

    retryWithBackoff(writeSdb, writeSdb.BatchPutAttributes, 5)(requestParams, function(err, data) {
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